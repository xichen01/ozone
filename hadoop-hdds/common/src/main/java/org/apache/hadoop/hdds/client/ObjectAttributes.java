/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ObjectAttributeProto;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.Map;

/**
 * Enum for file attributes
 */
public class ObjectAttributes {

  private enum AttributeType {
    USER,
    MTIME,
    CTIME
  }

  private final EnumMap<AttributeType, Object> attributes = new EnumMap<>(AttributeType.class);

  public void setUsername(String username) {
    attributes.put(AttributeType.USER, username);
  }

  public void setMtime(Long mtime) {
    attributes.put(AttributeType.MTIME, mtime);
  }

  public void setCtime(Long ctime) {
    attributes.put(AttributeType.CTIME, ctime);
  }

  @Nullable public String getUsername() {
    return (String) attributes.get(AttributeType.USER);
  }

  @Nullable public Long getMtime() {
    return (Long) attributes.get(AttributeType.MTIME);
  }

  @Nullable public Long getCtime() {
    return (Long) attributes.get(AttributeType.CTIME);
  }

  public boolean hasUsername() {
    return attributes.containsKey(AttributeType.USER);
  }

  public boolean hasMtime() {
    return attributes.containsKey(AttributeType.MTIME);
  }

  public boolean hasCtime() {
    return attributes.containsKey(AttributeType.CTIME);
  }

  public ObjectAttributeProto toProto() {
    ObjectAttributeProto.Builder builder = ObjectAttributeProto.newBuilder();

    for (Map.Entry<AttributeType, Object> entry : attributes.entrySet()) {
      if (entry.getValue() == null) {
        break;
      }
      switch (entry.getKey()) {
      case USER:
        builder.setUsername((String) entry.getValue());
        break;
      case MTIME:
        builder.setMtime((Long) entry.getValue());
        break;
      case CTIME:
        builder.setCtime((Long) entry.getValue());
        break;
      default:
        throw new IllegalArgumentException("Unsupported attribute: " + entry.getKey());
      }
    }

    return builder.build();
  }

  public static ObjectAttributes fromProto(ObjectAttributeProto proto) {
    ObjectAttributes objectAttributes = new ObjectAttributes();

    if (proto.hasUsername()) {
      objectAttributes.setUsername(proto.getUsername());
    }
    if (proto.hasMtime()) {
      objectAttributes.setMtime(proto.getMtime());
    }
    if (proto.hasCtime()) {
      objectAttributes.setCtime(proto.getCtime());
    }

    return objectAttributes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ObjectAttributes{");

    if (hasUsername()) {
      sb.append("username='").append(getUsername()).append('\'');
    }
    if (hasMtime()) {
      if (hasUsername()) {
        sb.append(", ");
      }
      sb.append("mtime=").append(getMtime());
    }
    if (hasCtime()) {
      if (hasUsername()) {
        sb.append(", ");
      }
      sb.append("ctime=").append(getCtime());
    }

    sb.append('}');
    return sb.toString();
  }
}