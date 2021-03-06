/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseDataValueVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.BuffersIterator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVectorDefinitionSetter;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.NullableMapReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;

public class NullableMapVector extends MapVector implements FieldVector {

  private final NullableMapReaderImpl reader = new NullableMapReaderImpl(this);

  protected final BitVector bits;

  private final List<BufferBacked> innerVectors;
  private final DictionaryEncoding dictionary;

  private final Accessor accessor;
  private final Mutator mutator;

  public NullableMapVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, null, callBack);
  }

  public NullableMapVector(String name, BufferAllocator allocator, DictionaryEncoding dictionary, CallBack callBack) {
    super(name, checkNotNull(allocator), callBack);
    this.bits = new BitVector("$bits$", allocator);
    this.innerVectors = Collections.unmodifiableList(Arrays.<BufferBacked>asList(bits));
    this.dictionary = dictionary;
    this.accessor = new Accessor();
    this.mutator = new Mutator();
  }

  @Override
  public Field getField() {
    Field f = super.getField();
    return new Field(f.getName(), true, f.getType(), f.getChildren());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    BaseDataValueVector.load(fieldNode, getFieldInnerVectors(), ownBuffers);
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public void loadFieldBuffers(BuffersIterator buffersIterator, ArrowBuf buf) {
    buffersIterator.next();
    ArrowBuf bitsData = buf.slice((int) buffersIterator.offset(), (int) buffersIterator.length());
    bits.load(bitsData);
    for (FieldVector child : getChildrenFromFields()) {
      child.loadFieldBuffers(buffersIterator, buf);
    }
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return BaseDataValueVector.unload(getFieldInnerVectors());
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return innerVectors;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new NullableMapVector(name, allocator, dictionary, null), false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new NullableMapTransferPair(this, (NullableMapVector) to, true);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new NullableMapVector(ref, allocator, dictionary, null), false);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new NullableMapTransferPair(this, new NullableMapVector(ref, allocator, dictionary, callBack), false);
  }

  protected class NullableMapTransferPair extends MapTransferPair {

    private NullableMapVector target;

    protected NullableMapTransferPair(NullableMapVector from, NullableMapVector to, boolean allocate) {
      super(from, to, allocate);
      this.target = to;
    }

    @Override
    public void transfer() {
      bits.transferTo(target.bits);
      super.transfer();
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      target.bits.copyFromSafe(fromIndex, toIndex, bits);
      super.copyValueSafe(fromIndex, toIndex);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      bits.splitAndTransferTo(startIndex, length, target.bits);
      super.splitAndTransfer(startIndex, length);
    }
  }

  @Override
  public int getValueCapacity() {
    return Math.min(bits.getValueCapacity(), super.getValueCapacity());
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return ObjectArrays.concat(bits.getBuffers(clear), super.getBuffers(clear), ArrowBuf.class);
  }

  @Override
  public void close() {
    bits.close();
    super.close();
  }

  @Override
  public void clear() {
    bits.clear();
    super.clear();
  }


  @Override
  public int getBufferSize(){
    return super.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSizeFor(valueCount)
        + bits.getBufferSizeFor(valueCount);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    super.setInitialCapacity(numRecords);
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = super.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    return success;
  }
  public final class Accessor extends MapVector.Accessor  {
    final BitVector.Accessor bAccessor = bits.getAccessor();

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return super.getObject(index);
      }
    }

    @Override
    public void get(int index, ComplexHolder holder) {
      holder.isSet = isSet(index);
      super.get(index, holder);
    }

    @Override
    public int getNullCount() {
      return bits.getAccessor().getNullCount();
    }

    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bAccessor.get(index);
    }

  }

  public final class Mutator extends MapVector.Mutator implements NullableVectorDefinitionSetter {

    int lastSet;

    private Mutator(){
    }

    @Override
    public void setIndexDefined(int index){
      bits.getMutator().setSafe(index, 1);
      lastSet = index;
    }

    public void setNull(int index){
      bits.getMutator().setSafe(index, 0);
    }

    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      super.setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
      lastSet = valueCount;
    }

    @Override
    public void generateTestData(int valueCount){
      super.generateTestData(valueCount);
      bits.getMutator().generateTestDataAlt(valueCount);
    }

    @Override
    public void reset(){
      bits.getMutator().setValueCount(0);
    }

  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

}
