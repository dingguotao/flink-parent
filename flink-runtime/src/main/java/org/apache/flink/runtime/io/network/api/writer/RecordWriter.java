/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after all records have
 * been written with {@link #emit(IOReadableWritable)}. This ensures that all produced records are
 * written to the output stream (incl. partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
/*********************
 * clouding 注释: 2022/7/23 21:58
 *  	    负责将Task数据输出到下游, 写出的数据就是StreamRecord
 *  	    实现类有两个:
 *  	        ChannelSelectorRecordWriter: 根据分区策略,将数据写入到subPartition
 *  	        BroadcastRecordWriter: 写出到编号是0的subPartition
 *********************/
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

    /** Default name for the output flush thread, if no name with a task reference is given. */
    @VisibleForTesting
    public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

    // clouding 注释: 2022/1/26 15:10
    //          这个就是ResultPartitionWriter,是上游写出数据的
    private final ResultPartitionWriter targetPartition;

    // clouding 注释: 2022/1/26 15:10
    //          下游有多少的chennel需要写入的总数.
    protected final int numberOfChannels;

    // clouding 注释: 2022/7/23 21:59
    //          负责把 StreamRecord 序列化成二进制写入Buffer,
    //          唯一的实现类 {@link SpanningRecordSerializer}
    protected final RecordSerializer<T> serializer;

    protected final Random rng = new XORShiftRandom();

    private Counter numBytesOut = new SimpleCounter();

    private Counter numBuffersOut = new SimpleCounter();

    protected Meter idleTimeMsPerSecond = new MeterView(new SimpleCounter());

    private final boolean flushAlways;

    /** The thread that periodically flushes the output, to give an upper latency bound. */
    // clouding 注释: 2022/1/26 15:12
    //          定时数据刷新器. 是一个单独的线程, 定时刷新数据到buffer
    @Nullable private final OutputFlusher outputFlusher;

    /**
     * To avoid synchronization overhead on the critical path, best-effort error tracking is enough
     * here.
     */
    private Throwable flusherException;

    RecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        this.targetPartition = writer;
        this.numberOfChannels = writer.getNumberOfSubpartitions();

        this.serializer = new SpanningRecordSerializer<T>();

        checkArgument(timeout >= -1);
        this.flushAlways = (timeout == 0);
        if (timeout == -1 || timeout == 0) {
            outputFlusher = null;
        } else {
            String threadName =
                    taskName == null
                            ? DEFAULT_OUTPUT_FLUSH_THREAD_NAME
                            : DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

            // clouding 注释: 2022/1/26 15:13
            //          刷新数据
            outputFlusher = new OutputFlusher(threadName, timeout);
            outputFlusher.start();
        }
    }

    protected void emit(T record, int targetChannel) throws IOException, InterruptedException {
        checkErroneous();

        // clouding 注释: 2022/8/15 00:21
        //          数据序列化后,写入 serializer, 数据存在serializer的ByteBuffer中
        serializer.serializeRecord(record);

        // Make sure we don't hold onto the large intermediate serialization buffer for too long
        // clouding 注释: 2022/8/15 00:21
        //          序列化在serializer中的数据,写入到对应subpartition
        if (copyFromSerializerToTargetChannel(targetChannel)) {
            // clouding 注释: 2022/7/23 22:26
            //          执行成功后,会做些serializer中数据清理, 数据最开始是写在了serializer中
            serializer.prune();
        }
    }

    /**
     * @param targetChannel
     * @return <tt>true</tt> if the intermediate serialization buffer should be pruned
     */
    protected boolean copyFromSerializerToTargetChannel(int targetChannel)
            throws IOException, InterruptedException {
        // We should reset the initial position of the intermediate serialization buffer before
        // copying, so the serialization results can be copied to multiple target buffers.
        // clouding 注释: 2022/8/15 00:23
        //          reset,从头开始复制数据
        serializer.reset();

        // clouding 注释: 2022/7/23 22:29
        //          是否执行清理
        boolean pruneTriggered = false;
        // clouding 注释: 2022/10/1 23:35
        //          这里会获取一个bufferBuilder,如果已经有,就复用,没有就申请
        BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
        SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
        while (result.isFullBuffer()) {
            // clouding 注释: 2022/10/1 23:41
            //          这个buffer 满了
            finishBufferBuilder(bufferBuilder);

            // If this was a full record, we are done. Not breaking out of the loop at this point
            // will lead to another buffer request before breaking out (that would not be a
            // problem per se, but it can lead to stalls in the pipeline).
            // clouding 注释: 2022/8/15 00:26
            //          如果数据读完了,刚好是条完整记录,就返回
            if (result.isFullRecord()) {
                pruneTriggered = true;
                emptyCurrentBufferBuilder(targetChannel);
                break;
            }

            // clouding 注释: 2022/8/15 00:26
            //          不是完整的记录,说明后半部分还在 serializer中,再申请buffer去读
            bufferBuilder = requestNewBufferBuilder(targetChannel);
            result = serializer.copyToBufferBuilder(bufferBuilder);
        }
        checkState(!serializer.hasSerializedData(), "All data should be written at once");

        // clouding 注释: 2022/10/1 23:42
        //          这个 flushAlways 决定了什么时候发送数据通知.
        //          有两种:
        //              1. timeout == 0, 那么每次都flush.
        //              2. 带 timeout的,是定时任务, 来触发所有targetChannel的flush方法. targetPartition.flushAll(); 默认就是这个
        //          flush() 就是 让NetworkSequenceViewReader组件开始消费ResultSubPartition中的BufferConsumer对象
        if (flushAlways) {
            // clouding 注释: 2022/7/23 22:31
            //          发送数据
            flushTargetPartition(targetChannel);
        }
        return pruneTriggered;
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
            for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
                tryFinishCurrentBufferBuilder(targetChannel);

                // Retain the buffer so that it can be recycled by each channel of targetPartition
                targetPartition.addBufferConsumer(
                        eventBufferConsumer.copy(), targetChannel, isPriorityEvent);
            }

            if (flushAlways) {
                flushAll();
            }
        }
    }

    public void flushAll() {
        targetPartition.flushAll();
    }

    protected void flushTargetPartition(int targetChannel) {
        targetPartition.flush(targetChannel);
    }

    /** Sets the metric group for this RecordWriter. */
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        numBytesOut = metrics.getNumBytesOutCounter();
        numBuffersOut = metrics.getNumBuffersOutCounter();
        idleTimeMsPerSecond = metrics.getIdleTimeMsPerSecond();
    }

    protected void finishBufferBuilder(BufferBuilder bufferBuilder) {
        numBytesOut.inc(bufferBuilder.finish());
        numBuffersOut.inc();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return targetPartition.getAvailableFuture();
    }

    /** This is used to send regular records. */
    public abstract void emit(T record) throws IOException, InterruptedException;

    /** This is used to send LatencyMarks to a random target channel. */
    public abstract void randomEmit(T record) throws IOException, InterruptedException;

    /** This is used to broadcast streaming Watermarks in-band with records. */
    public abstract void broadcastEmit(T record) throws IOException, InterruptedException;

    /**
     * The {@link BufferBuilder} may already exist if not filled up last time, otherwise we need
     * request a new one for this target channel.
     */
    abstract BufferBuilder getBufferBuilder(int targetChannel)
            throws IOException, InterruptedException;

    /**
     * Marks the current {@link BufferBuilder} as finished if present and clears the state for next
     * one.
     */
    abstract void tryFinishCurrentBufferBuilder(int targetChannel);

    /** Marks the current {@link BufferBuilder} as empty for the target channel. */
    abstract void emptyCurrentBufferBuilder(int targetChannel);

    /**
     * Marks the current {@link BufferBuilder} as finished and releases the resources for the target
     * channel.
     */
    abstract void closeBufferBuilder(int targetChannel);

    /** Closes the {@link BufferBuilder}s for all the channels. */
    public abstract void clearBuffers();

    /** Closes the writer. This stops the flushing thread (if there is one). */
    public void close() {
        clearBuffers();
        // make sure we terminate the thread in any case
        if (outputFlusher != null) {
            outputFlusher.terminate();
            try {
                outputFlusher.join();
            } catch (InterruptedException e) {
                // ignore on close
                // restore interrupt flag to fast exit further blocking calls
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Notifies the writer that the output flusher thread encountered an exception.
     *
     * @param t The exception to report.
     */
    private void notifyFlusherException(Throwable t) {
        if (flusherException == null) {
            LOG.error("An exception happened while flushing the outputs", t);
            flusherException = t;
        }
    }

    protected void checkErroneous() throws IOException {
        if (flusherException != null) {
            throw new IOException(
                    "An exception happened while flushing the outputs", flusherException);
        }
    }

    protected void addBufferConsumer(BufferConsumer consumer, int targetChannel)
            throws IOException {
        targetPartition.addBufferConsumer(consumer, targetChannel);
    }

    /*********************
     * clouding 注释: 2022/1/26 14:59
     *  	    尝试去获取bufferBuilder, 如果获取不到,会阻塞线程,会引起反压
     *********************/
    /** Requests a new {@link BufferBuilder} for the target channel and returns it. */
    public BufferBuilder requestNewBufferBuilder(int targetChannel)
            throws IOException, InterruptedException {
        // clouding 注释: 2022/10/1 23:37
        //          尝试给 targetChannel 拿到一个Buffer, 从bufferPool中申请buffer
        BufferBuilder builder = targetPartition.tryGetBufferBuilder(targetChannel); // todo 尝试去获取bufferBuilder
        if (builder == null) {
            long start = System.currentTimeMillis();
            // clouding 注释: 2022/1/26 15:00
            //          阻塞获取,会引起反压
            builder = targetPartition.getBufferBuilder(targetChannel);
            idleTimeMsPerSecond.markEvent(System.currentTimeMillis() - start);
        }
        return builder;
    }

    @VisibleForTesting
    public Meter getIdleTimeMsPerSecond() {
        return idleTimeMsPerSecond;
    }

    // ------------------------------------------------------------------------

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
        }

        public void terminate() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        // propagate this if we are still running, because it should not happen
                        // in that case
                        if (running) {
                            throw new Exception(e);
                        }
                    }

                    // any errors here should let the thread come to a halt and be
                    // recognized by the writer
                    flushAll();
                }
            } catch (Throwable t) {
                notifyFlusherException(t);
            }
        }
    }

    @VisibleForTesting
    ResultPartitionWriter getTargetPartition() {
        return targetPartition;
    }
}
