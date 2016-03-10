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
package io.netty.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A MutableWrappedByteBuf that also maintains a metric of the number of huge buffer bytes and counts.
 */
public class UnmanagedBuffer extends AbstractByteBuf {

  private final long addr;
  private final int length;

  public UnmanagedBuffer(long addr, int length) {
    super(length);
    this.addr = addr;
    this.length = length;
  }

  @Override
  protected byte _getByte(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected short _getShort(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int _getUnsignedMedium(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int _getInt(int i) {

    throw new UnsupportedOperationException();
  }

  @Override
  protected long _getLong(int i) {

    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setByte(int i, int i1) {
    throw new UnsupportedOperationException();

  }

  @Override
  protected void _setShort(int i, int i1) {

    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setMedium(int i, int i1) {
    throw new UnsupportedOperationException();

  }

  @Override
  protected void _setInt(int i, int i1) {

    throw new UnsupportedOperationException();
  }

  @Override
  protected void _setLong(int i, long l) {

    throw new UnsupportedOperationException();
  }

  @Override
  public int capacity() {
    return length;
  }

  @Override
  public ByteBuf capacity(int i) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBufAllocator alloc() {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  @Override
  public ByteBuf unwrap() {

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDirect() {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int i, ByteBuf byteBuf, int i1, int i2) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int i, byte[] bytes, int i1, int i2) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int i, ByteBuffer byteBuffer) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int i, OutputStream outputStream, int i1) throws IOException {

    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytes(int i, GatheringByteChannel gatheringByteChannel, int i1) throws IOException {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int i, ByteBuf byteBuf, int i1, int i2) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int i, byte[] bytes, int i1, int i2) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int i, ByteBuffer byteBuffer) {

    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int i, InputStream inputStream, int i1) throws IOException {

    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int i, ScatteringByteChannel scatteringByteChannel, int i1) throws IOException {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy(int i, int i1) {

    throw new UnsupportedOperationException();
  }

  @Override
  public int nioBufferCount() {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer nioBuffer(int i, int i1) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer internalNioBuffer(int i, int i1) {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] nioBuffers(int i, int i1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasArray() {

    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] array() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int arrayOffset() {

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasMemoryAddress() {
    return true;
  }

  @Override
  public long memoryAddress() {
    return addr;
  }

  @Override
  public ByteBuf retain(int i) {

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean release() {

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean release(int decrement) {

    throw new UnsupportedOperationException();
  }

  @Override
  public int refCnt() {

    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retain() {
    throw new UnsupportedOperationException();
  }
}
