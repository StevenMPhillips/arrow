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
package org.apache.arrow.vector.types;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.Table;
import org.apache.arrow.flatbuf.Binary;
import org.apache.arrow.flatbuf.Bit;
import org.apache.arrow.flatbuf.Date;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Time;
import org.apache.arrow.flatbuf.Timestamp;
import org.apache.arrow.flatbuf.Tuple;
import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.flatbuf.Union;
import org.apache.arrow.flatbuf.Utf8;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableDateVector;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableIntervalDayVector;
import org.apache.arrow.vector.NullableIntervalYearVector;
import org.apache.arrow.vector.NullableSmallIntVector;
import org.apache.arrow.vector.NullableTimeStampVector;
import org.apache.arrow.vector.NullableTimeVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.NullableUInt1Vector;
import org.apache.arrow.vector.NullableUInt2Vector;
import org.apache.arrow.vector.NullableUInt4Vector;
import org.apache.arrow.vector.NullableUInt8Vector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.BigIntWriterImpl;
import org.apache.arrow.vector.complex.impl.BitWriterImpl;
import org.apache.arrow.vector.complex.impl.DateWriterImpl;
import org.apache.arrow.vector.complex.impl.Float4WriterImpl;
import org.apache.arrow.vector.complex.impl.Float8WriterImpl;
import org.apache.arrow.vector.complex.impl.IntWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalDayWriterImpl;
import org.apache.arrow.vector.complex.impl.IntervalYearWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapWriter;
import org.apache.arrow.vector.complex.impl.SmallIntWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeStampWriterImpl;
import org.apache.arrow.vector.complex.impl.TimeWriterImpl;
import org.apache.arrow.vector.complex.impl.TinyIntWriterImpl;
import org.apache.arrow.vector.complex.impl.UInt1WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt2WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt4WriterImpl;
import org.apache.arrow.vector.complex.impl.UInt8WriterImpl;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.util.CallBack;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Types {

  public static final byte MAP_TYPE_TYPE = Type.Tuple;
  public static final byte TINYINT_TYPE_TYPE = Type.Int;
  public static final byte SMALLINT_TYPE_TYPE = Type.Int;
  public static final byte INT_TYPE_TYPE = Type.Int;
  public static final byte BIGINT_TYPE_TYPE = Type.Int;
  public static final byte UINT1_TYPE_TYPE = Type.Int;
  public static final byte UINT2_TYPE_TYPE = Type.Int;
  public static final byte UINT4_TYPE_TYPE = Type.Int;
  public static final byte UINT8_TYPE_TYPE = Type.Int;
  public static final byte DATE_TYPE_TYPE = Type.Date;
  public static final byte TIME_TYPE_TYPE = Type.Time;
  public static final byte TIMESTAMP_TYPE_TYPE = Type.Timestamp;
  public static final byte INTERVALDAY_TYPE_TYPE = Type.IntervalDay;
  public static final byte INTERVALYEAR_TYPE_TYPE = Type.IntervalYear;
  public static final byte FLOAT4_TYPE_TYPE = Type.FloatingPoint;
  public static final byte FLOAT8_TYPE_TYPE = Type.FloatingPoint;
  public static final byte LIST_TYPE_TYPE = Type.List;
  public static final byte VARCHAR_TYPE_TYPE = Type.Utf8;
  public static final byte VARBINARY_TYPE_TYPE = Type.Binary;
  public static final byte DECIMAL_TYPE_TYPE = Type.Decimal;
  public static final byte UNION_TYPE_TYPE = Type.Union;
  public static final byte BIT_TYPE_TYPE = Type.Bit;

  public enum MinorType {
    NULL(Type.NONE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new ZeroVector();
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return null;
      }
    },
    MAP(MAP_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
         return new MapVector(name, allocator, callBack);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SingleMapWriter((MapVector) vector);
      }
    },   //  an empty map column.  Useful for conceptual setup.  Children listed within here

    TINYINT(TINYINT_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTinyIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TinyIntWriterImpl((NullableTinyIntVector) vector);
      }
    },   //  single byte signed integer
    SMALLINT(SMALLINT_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new SmallIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new SmallIntWriterImpl((NullableSmallIntVector) vector);
      }
    },   //  two byte signed integer
    INT(INT_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntWriterImpl((NullableIntVector) vector);
      }
    },   //  four byte signed integer
    BIGINT(BIGINT_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableBigIntVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BigIntWriterImpl((NullableBigIntVector) vector);
      }
    },   //  eight byte signed integer
    DATE(DATE_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableDateVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new DateWriterImpl((NullableDateVector) vector);
      }
    },   //  days since 4713bc
    TIME(TIME_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTimeVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeWriterImpl((NullableTimeVector) vector);
      }
    },   //  time in micros before or after 2000/1/1
    TIMESTAMP(TIMESTAMP_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableTimeStampVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new TimeStampWriterImpl((NullableTimeStampVector) vector);
      }
    },
    INTERVALDAY(INTERVALDAY_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntervalDayVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalDayWriterImpl((NullableIntervalDayVector) vector);
      }
    },
    INTERVALYEAR(INTERVALYEAR_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableIntervalDayVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new IntervalYearWriterImpl((NullableIntervalYearVector) vector);
      }
    },
    FLOAT4(FLOAT4_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableFloat4Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float4WriterImpl((NullableFloat4Vector) vector);
      }
    },   //  4 byte ieee 754
    FLOAT8(FLOAT8_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableFloat8Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new Float8WriterImpl((NullableFloat8Vector) vector);
      }
    },   //  8 byte ieee 754
    BIT(BIT_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableBitVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new BitWriterImpl((NullableBitVector) vector);
      }
    },  //  single bit value (boolean)
    VARCHAR(VARCHAR_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableVarCharVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarCharWriterImpl((NullableVarCharVector) vector);
      }
    },   //  utf8 variable length string
    VARBINARY(VARBINARY_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableVarBinaryVector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarBinaryWriterImpl((NullableVarBinaryVector) vector);
      }
    },   //  variable length binary
    DECIMAL(DECIMAL_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableDecimalVector(name, allocator, precisionScale[0], precisionScale[1]);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new VarBinaryWriterImpl((NullableVarBinaryVector) vector);
      }
    },   //  variable length binary
    UINT1(UINT1_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt1Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt1WriterImpl((NullableUInt1Vector) vector);
      }
    },  //  unsigned 1 byte integer
    UINT2(UINT2_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt2Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt2WriterImpl((NullableUInt2Vector) vector);
      }
    },  //  unsigned 2 byte integer
    UINT4(UINT4_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt4Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt4WriterImpl((NullableUInt4Vector) vector);
      }
    },   //  unsigned 4 byte integer
    UINT8(UINT8_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new NullableUInt8Vector(name, allocator);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UInt8WriterImpl((NullableUInt8Vector) vector);
      }
    },   //  unsigned 8 byte integer
    LIST(LIST_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new ListVector(name, allocator, callBack);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionListWriter((ListVector) vector);
      }
    },
    UNION(UNION_TYPE_TYPE) {
      @Override
      public ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale) {
        return new UnionVector(name, allocator, callBack);
      }

      @Override
      public FieldWriter getNewFieldWriter(ValueVector vector) {
        return new UnionWriter((UnionVector) vector);
      }
    };

    private final byte typeType;

    MinorType(byte typeType) {
      this.typeType = typeType;
    }

    public byte getTypeType() {
      return typeType;
    }

    public abstract ValueVector getNewVector(String name, BufferAllocator allocator, CallBack callBack, int... precisionScale);

    public abstract FieldWriter getNewFieldWriter(ValueVector vector);
  }
}
