/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.util;

import com.google.flatbuffers.FlatBufferBuilder;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.format.dataheaders.Buffer;
import org.apache.arrow.format.dataheaders.BufferList;
import org.apache.arrow.metadata.Field;
import org.apache.arrow.metadata.Int;
import org.apache.arrow.metadata.Metadata;
import org.apache.arrow.metadata.Struct;
import org.apache.arrow.metadata.Type;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MinorType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class FlatBufUtil {

  public static ByteBuffer getMetadata(List<ValueVector> vectors) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int version = 1;
    int metadata_key = 1;
    int[] fieldOffsets = new int[vectors.size()];
    for (int i = 0; i < fieldOffsets.length; i++) {
      fieldOffsets[0] = getField(builder, vectors.get(i));
    }
    int fieldsOffset = Metadata.createFieldsVector(builder, fieldOffsets);
    int metadataOffset = Metadata.createMetadata(builder, version, metadata_key, fieldsOffset);
    builder.finish(metadataOffset);
    return copyBuffer(builder.dataBuffer());
  }

  public static int getField(FlatBufferBuilder builder, ValueVector vector) {
    MinorType type = vector.getField().getType().getMinorType();
    DataMode mode = vector.getField().getType().getMode();

    switch (type) {
    case INT:
      switch (mode) {
      case REQUIRED:
        return getField(builder, (IntVector) vector);
      }
    }
    throw new UnsupportedOperationException(String.format("%s:%s", type, mode));
  }

  public static int getField(FlatBufferBuilder builder, IntVector intVector) {
    int nameOffset = builder.createString(intVector.getField().getName());
    int typeOffset = Int.createInt(builder, 16, true);
    int childrenOffset = Field.createChildrenVector(builder, new int[] {});
    return Field.createField(builder, nameOffset, false, Type.Int, typeOffset, childrenOffset);
  }

  public static int[] getBuffers(FlatBufferBuilder builder, List<ArrowBuf> buffers) {
    int[] offsets = new int[buffers.size()];
    for (int i = 0; i < offsets.length; i++) {
      ArrowBuf buf = buffers.get(i);
      offsets[i] = Buffer.createBuffer(builder, buf.memoryAddress(), buf.writerIndex());
    }
    return offsets;
  }

  public static ByteBuffer byteBuffer(long addr, int length) throws NoSuchFieldException, IllegalAccessException {
    java.lang.reflect.Field address = java.nio.Buffer.class.getDeclaredField("address");
    address.setAccessible(true);
    java.lang.reflect.Field capacity = java.nio.Buffer.class.getDeclaredField("capacity");
    capacity.setAccessible(true);
    java.lang.reflect.Field limit = java.nio.Buffer.class.getDeclaredField("limit");
    limit.setAccessible(true);

    ByteBuffer bb = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
    address.setLong(bb, addr);
    capacity.setInt(bb, length);
    limit.setInt(bb, length);
    return bb;
  }

  public static int getBuffer(FlatBufferBuilder builder, ArrowBuf buf) {
    return Buffer.createBuffer(builder, buf.memoryAddress(), buf.writerIndex());
  }

  public static ByteBuffer getDataHeader(List<ValueVector> vectors) {
    List<ArrowBuf> buffers = new ArrayList<>();
    for (ValueVector vector : vectors) {
      for (ArrowBuf buf : vector.getBuffers(false)) {
        buffers.add(buf);
      }
    }
    FlatBufferBuilder builder = new FlatBufferBuilder();

    int[] bufferOffsets = new int[buffers.size()];
    for (int i = 0; i < bufferOffsets.length; i++) {
      bufferOffsets[i] = getBuffer(builder, buffers.get(i));
    }
    int buffersOffset = BufferList.createBuffersVector(builder, bufferOffsets);
    int bufferListOffset = BufferList.createBufferList(builder, buffersOffset);

    builder.finish(bufferListOffset);
    return copyBuffer(builder.dataBuffer());
  }

  private static ByteBuffer copyBuffer(ByteBuffer src) {
    ByteBuffer dst = ByteBuffer.allocateDirect(src.capacity());
    for (int i = 0; i < src.capacity(); i++) {
      dst.put(src.get(i));
    }
    dst.position(0);
    return dst;
  }
}
