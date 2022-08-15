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
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Record serializer which serializes the complete record to an intermediate data serialization
 * buffer and copies this buffer to target buffers one-by-one using {@link
 * #copyToBufferBuilder(BufferBuilder)}.
 *
 * @param <T> The type of the records that are serialized.
 */
/*********************
 * clouding 注释: 2022/7/23 22:34
 *  	    数据记录的序列化器, 可以支持跨内存段的序列化器
 *********************/
public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

    /** Flag to enable/disable checks, if buffer not set/full or pending serialization. */
    private static final boolean CHECKED = false;

    /** Intermediate data serialization. */
    private final DataOutputSerializer serializationBuffer;

    /** Intermediate buffer for data serialization (wrapped from {@link #serializationBuffer}). */
    // clouding 注释: 2022/7/23 22:42
    //          Java中原生的 ByteBuffer
    private ByteBuffer dataBuffer;

    public SpanningRecordSerializer() {
        serializationBuffer = new DataOutputSerializer(128);

        // ensure initial state with hasRemaining false (for correct
        // continueWritingWithNextBufferBuilder logic)
        dataBuffer = serializationBuffer.wrapAsByteBuffer();
    }

    /**
     * Serializes the complete record to an intermediate data serialization buffer.
     *
     * @param record the record to serialize
     */
    // clouding 注释: 2022/7/23 22:01
    //          序列化 record 到这个类的 dataBuffer 中,
    //          dataBuffer 中, limit 是Buffer内存段的长度, position是写入位置的偏移量
    @Override
    public void serializeRecord(T record) throws IOException {
        if (CHECKED) {
            if (dataBuffer.hasRemaining()) {
                throw new IllegalStateException("Pending serialization of previous record.");
            }
        }

        // clouding 注释: 2022/8/15 00:06
        //          清理之前的数据,就是修改 buffer中position的位置
        serializationBuffer.clear();
        // the initial capacity of the serialization buffer should be no less than 4
        // clouding 注释: 2022/8/15 00:06
        //          跳过4个字节,后面用来放数据的长度
        serializationBuffer.skipBytesToWrite(4);

        // write data and length
        // clouding 注释: 2022/8/15 00:06
        //          写入数据
        record.write(serializationBuffer);

        // clouding 注释: 2022/8/15 00:14
        //          写入长度,并且将数据跳到最后
        int len = serializationBuffer.length() - 4;
        serializationBuffer.setPosition(0);
        serializationBuffer.writeInt(len);
        serializationBuffer.skipBytesToWrite(len);

        // clouding 注释: 2022/8/15 00:16
        //          转换为 ByteBuffer
        dataBuffer = serializationBuffer.wrapAsByteBuffer();
    }

    /**
     * Copies an intermediate data serialization buffer into the target BufferBuilder.
     *
     * @param targetBuffer the target BufferBuilder to copy to
     * @return how much information was written to the target buffer and whether this buffer is full
     */
    // clouding 注释: 2022/7/23 22:01
    //          将 dataBuffer 数据写入到 targetBuffer 中
    @Override
    public SerializationResult copyToBufferBuilder(BufferBuilder targetBuffer) {
        targetBuffer.append(dataBuffer);
        targetBuffer.commit();

        // clouding 注释: 2022/7/23 22:48
        //          获取 ByteBuffer 的写入状态
        return getSerializationResult(targetBuffer);
    }

    private SerializationResult getSerializationResult(BufferBuilder targetBuffer) {
        if (dataBuffer.hasRemaining()) {
            // clouding 注释: 2022/7/23 22:48
            //          Record 还有剩余,但是BufferBuilder没空间了
            return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
        }
        return !targetBuffer.isFull()
                // clouding 注释: 2022/7/23 22:49
                //       Record写完了, 但是 BufferBuilder还有空间
                ? SerializationResult.FULL_RECORD
                // clouding 注释: 2022/7/23 22:50
                //          Record写完了, 并且 BufferBuilder写满了
                : SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
    }

    @Override
    public void reset() {
        dataBuffer.position(0);
    }

    @Override
    public void prune() {
        serializationBuffer.pruneBuffer();
        dataBuffer = serializationBuffer.wrapAsByteBuffer();
    }

    @Override
    public boolean hasSerializedData() {
        return dataBuffer.hasRemaining();
    }
}
