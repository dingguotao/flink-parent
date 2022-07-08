/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>>
        extends AbstractUdfStreamOperator<OUT, SRC> {

    private static final long serialVersionUID = 1L;

    private transient SourceFunction.SourceContext<OUT> ctx;

    private transient volatile boolean canceledOrStopped = false;

    private transient volatile boolean hasSentMaxWatermark = false;

    public StreamSource(SRC sourceFunction) {
        super(sourceFunction);

        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    public void run(
            final Object lockingObject,
            final StreamStatusMaintainer streamStatusMaintainer,
            final OperatorChain<?, ?> operatorChain)
            throws Exception {

        run(lockingObject, streamStatusMaintainer, output, operatorChain);
    }

    public void run(
            final Object lockingObject,
            final StreamStatusMaintainer streamStatusMaintainer,
            final Output<StreamRecord<OUT>> collector,
            final OperatorChain<?, ?> operatorChain)
            throws Exception {

        // clouding 注释: 2021/9/21 14:39
        //          时间语义
        final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

        // clouding 注释: 2021/10/25 22:03
        //          获取配置
        final Configuration configuration =
                this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
        final long latencyTrackingInterval =
                getExecutionConfig().isLatencyTrackingConfigured()
                        ? getExecutionConfig().getLatencyTrackingInterval()
                        : configuration.getLong(MetricOptions.LATENCY_INTERVAL);

        LatencyMarkerEmitter<OUT> latencyEmitter = null;
        if (latencyTrackingInterval > 0) {
            latencyEmitter =
                    new LatencyMarkerEmitter<>(
                            getProcessingTimeService(),
                            collector::emitLatencyMarker,
                            latencyTrackingInterval,
                            this.getOperatorID(),
                            getRuntimeContext().getIndexOfThisSubtask());
        }

        final long watermarkInterval =
                getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

        this.ctx =
                StreamSourceContexts.getSourceContext(
                        timeCharacteristic,
                        getProcessingTimeService(),
                        lockingObject,
                        streamStatusMaintainer,
                        collector,
                        watermarkInterval,
                        -1);

        try {
            // clouding 注释: 2021/10/25 22:04
            //          这里调用用户的source数据源，比如kafka
            userFunction.run(ctx);

            // if we get here, then the user function either exited after being done (finite source)
            // or the function was canceled or stopped. For the finite source case, we should emit
            // a final watermark that indicates that we reached the end of event-time, and end
            // inputs
            // of the operator chain
            if (!isCanceledOrStopped()) {
                // in theory, the subclasses of StreamSource may implement the BoundedOneInput
                // interface,
                // so we still need the following call to end the input
                synchronized (lockingObject) {
                    operatorChain.setIgnoreEndOfInput(false);
                    operatorChain.endInput(1);
                }
            }
        } finally {
            if (latencyEmitter != null) {
                latencyEmitter.close();
            }
        }
    }

    public void advanceToEndOfEventTime() {
        if (!hasSentMaxWatermark) {
            ctx.emitWatermark(Watermark.MAX_WATERMARK);
            hasSentMaxWatermark = true;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (!isCanceledOrStopped() && ctx != null) {
                advanceToEndOfEventTime();
            }
        } finally {
            // make sure that the context is closed in any case
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public void cancel() {
        // important: marking the source as stopped has to happen before the function is stopped.
        // the flag that tracks this status is volatile, so the memory model also guarantees
        // the happens-before relationship
        markCanceledOrStopped();
        userFunction.cancel();

        // the context may not be initialized if the source was never running.
        if (ctx != null) {
            ctx.close();
        }
    }

    /**
     * Marks this source as canceled or stopped.
     *
     * <p>This indicates that any exit of the {@link #run(Object, StreamStatusMaintainer, Output)}
     * method cannot be interpreted as the result of a finite source.
     */
    protected void markCanceledOrStopped() {
        this.canceledOrStopped = true;
    }

    /**
     * Checks whether the source has been canceled or stopped.
     *
     * @return True, if the source is canceled or stopped, false is not.
     */
    protected boolean isCanceledOrStopped() {
        return canceledOrStopped;
    }
}
