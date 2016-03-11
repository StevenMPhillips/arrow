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
package org.apache.arrow.vector;

import org.apache.arrow.format.dataheaders.Field;
import org.apache.arrow.format.dataheaders.FieldNode;
import org.apache.arrow.format.dataheaders.Message;
import org.apache.arrow.format.dataheaders.MessageHeader;
import org.apache.arrow.format.dataheaders.RecordBatch;
import org.apache.arrow.format.dataheaders.Schema;
import org.apache.arrow.format.dataheaders.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.FlatBufUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFlatBuf {

  static private List<ValueVector> vectors = new ArrayList<>();

  static {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    IntVector vector = new IntVector(MaterializedField.create("v1", new MajorType(MinorType.INT, DataMode.REQUIRED)), allocator);
    vector.allocateNew();
    for (int i = 0; i < 100; i++) {
      vector.getMutator().set(i, i);
    }
    vector.getMutator().setValueCount(100);
    vectors.add(vector);
    vector = new IntVector(MaterializedField.create("v2", new MajorType(MinorType.INT, DataMode.REQUIRED)), allocator);
    vector.allocateNew();
    for (int i = 0; i < 100; i++) {
      vector.getMutator().set(i, i);
    }
    vector.getMutator().setValueCount(100);
    vectors.add(vector);
  }

  @Test
  public void getMetadata() throws Exception {
    ByteBuffer buffer = FlatBufUtil.getSchema(vectors);
    Message message = Message.getRootAsMessage(buffer);
    assertEquals(MessageHeader.Schema, message.headerType());
    Schema schema = (Schema) message.header(new Schema());
    assertEquals(2, schema.fieldsLength());
    Field field1 = schema.fields(0);
    assertEquals("v1", field1.name());
    assertEquals(Type.Int, field1.typeType());
    assertFalse(field1.nullable());
    Field field2 = ((Schema) message.header(new Schema())).fields(1);
    assertEquals("v2", field2.name());
    assertEquals(Type.Int, field1.typeType());
    assertFalse(field1.nullable());
  }

  @Test
  public void getDataHeader() throws Exception {
    ByteBuffer buffer = FlatBufUtil.getRecordBatch(vectors, 100);
    Message message = Message.getRootAsMessage(buffer);
    assertEquals(MessageHeader.RecordBatch, message.headerType());
    RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
    assertEquals(100, recordBatch.length());
    assertEquals(2, recordBatch.buffersLength());
    assertEquals(2, recordBatch.nodesLength());
    FieldNode node1 = recordBatch.nodes(0);
    assertEquals(100, node1.length());
    assertEquals(0, node1.nullCount());
    FieldNode node2= recordBatch.nodes(1);
    assertEquals(100, node2.length());
    assertEquals(0, node2.nullCount());
  }
}
