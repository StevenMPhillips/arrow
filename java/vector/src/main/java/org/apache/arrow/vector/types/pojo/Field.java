/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types.pojo;


import com.google.flatbuffers.FlatBufferBuilder;

import java.util.List;

public class Field {
  private final String name;
  private final boolean nullable;
  private final ArrowType type;
  private final List<Field> children;

  public Field(String name, boolean nullable, ArrowType type, List<Field> children) {
    this.name = name;
    this.nullable = nullable;
    this.type = type;
    this.children = children;
  }

  public int getField(FlatBufferBuilder builder) {
    int nameOffset = builder.createString(name);
    org.apache.arrow.flatbuf.Field.startField(builder);
    org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    org.apache.arrow.flatbuf.Field.addNullable(builder, nullable);
    org.apache.arrow.flatbuf.Field.addTypeType(builder, type.getTypeType());
    org.apache.arrow.flatbuf.Field.addType(builder, type.getType(builder));
    if (children != null) {
      int[] childrenData = new int[children.size()];
      for (int i = 0; i < children.size(); i++) {
        childrenData[i] = children.get(i).getField(builder);
      }
      int childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, childrenData);
      org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    }
    return org.apache.arrow.flatbuf.Field.endField(builder);
  }

  public String getName() {
    return name;
  }

  public boolean isNullable() {
    return nullable;
  }

  public ArrowType getType() {
    return type;
  }

  public List<Field> getChildren() {
    return children;
  }
}
