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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Interface for turning sequences of memory segments into records. */
/*********************
 * clouding 注释: 2022/7/24 00:27
 *  	    实现类: SpillingAdaptiveSpanningRecordDeserializer, 和序列化器 {@link SpanningRecordSerializer} 类似
 *********************/
public interface RecordDeserializer<T extends IOReadableWritable> {

    /** Status of the deserialization result. */
    /*********************
     * clouding 注释: 2022/7/24 00:27
     *  	    和 {@link org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult} 类似
     *  	    有3种状态:
     *  	    PARTIAL_RECORD: 记录未被完全读取,但是缓冲区已经消费完成
     *  	    INTERMEDIATE_RECORD_FROM_BUFFER: 记录已经被完全读取,但是缓冲区还有数据
     *  	    LAST_RECORD_FROM_BUFFER: 记录被完全读取,并且缓冲区消费完成
     *********************/
    enum DeserializationResult {
        PARTIAL_RECORD(false, true),
        INTERMEDIATE_RECORD_FROM_BUFFER(true, false),
        LAST_RECORD_FROM_BUFFER(true, true);

        private final boolean isFullRecord;

        private final boolean isBufferConsumed;

        private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
            this.isFullRecord = isFullRecord;
            this.isBufferConsumed = isBufferConsumed;
        }

        public boolean isFullRecord() {
            return this.isFullRecord;
        }

        public boolean isBufferConsumed() {
            return this.isBufferConsumed;
        }
    }

    DeserializationResult getNextRecord(T target) throws IOException;

    void setNextBuffer(Buffer buffer) throws IOException;

    Buffer getCurrentBuffer();

    void clear();

    boolean hasUnfinishedData();

    /**
     * Gets the unconsumed buffer which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer might be null if the whole buffer was already consumed
     * before and there are no partial length or data remained in the end of buffer.
     */
    CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
}
