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

import org.apache.arrow.format.dataheaders.BufferList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.metadata.Metadata;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.FlatBufUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
    ByteBuffer buffer = FlatBufUtil.getMetadata(vectors);
    Metadata metadata = Metadata.getRootAsMetadata(buffer);
  }

  @Test
  public void getDataHeader() throws Exception {
    ByteBuffer buffer = FlatBufUtil.getDataHeader(vectors);
    BufferList bufferList = BufferList.getRootAsBufferList(buffer);
  }
}
