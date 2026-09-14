/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleTransition;

/** Lifecycle transition supported by the EC-only migration backport. */
@Immutable
public final class OmLCTransition implements OmLCAction {
  public static final String DEEP_ARCHIVE = "DEEP_ARCHIVE";

  private final Integer days;
  private final String date;
  private final String storageClass;

  private OmLCTransition(Builder builder) {
    this.days = builder.days;
    this.date = builder.date;
    this.storageClass = builder.storageClass;
  }

  @Nullable
  public Integer getDays() {
    return days;
  }

  @Nullable
  public String getDate() {
    return date;
  }

  public String getStorageClass() {
    return storageClass;
  }

  @Override
  public void valid(long creationTime) throws OMException {
    boolean hasDays = days != null;
    boolean hasDate = !StringUtils.isBlank(date);
    if (hasDays == hasDate) {
      throw new OMException("Invalid lifecycle configuration: Either 'Days' or 'Date' " +
          "should be specified, but not both or neither.", OMException.ResultCodes.INVALID_REQUEST);
    }
    if (hasDays && days < 0) {
      throw new OMException("'Days' for Transition action must be non-negative.",
          OMException.ResultCodes.INVALID_REQUEST);
    }
    if (hasDate) {
      try {
        ZonedDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME);
      } catch (DateTimeParseException e) {
        throw new OMException("Invalid lifecycle configuration: 'Date' must be in ISO 8601 format",
            OMException.ResultCodes.INVALID_REQUEST);
      }
    }
    if (!DEEP_ARCHIVE.equalsIgnoreCase(storageClass)) {
      throw new OMException("Only DEEP_ARCHIVE lifecycle transitions are supported for EC migration.",
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  /** Returns whether the transition's time condition has elapsed for a key. */
  public boolean shouldExecute(long modificationTime) {
    if (days != null) {
      return System.currentTimeMillis() >= modificationTime + TimeUnit.DAYS.toMillis(days);
    }
    try {
      return Instant.now().compareTo(Instant.parse(date)) >= 0;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public ActionType getActionType() {
    return ActionType.TRANSITION;
  }

  @Override
  public LifecycleAction getProtobuf() {
    LifecycleTransition.Builder builder = LifecycleTransition.newBuilder()
        .setStorageClass(storageClass);
    if (days != null) {
      builder.setDays(days);
    }
    if (date != null) {
      builder.setDate(date);
    }
    return LifecycleAction.newBuilder().setTransition(builder).build();
  }

  public static OmLCTransition getFromProtobuf(LifecycleTransition transition) {
    Builder builder = new Builder().setStorageClass(transition.getStorageClass());
    if (transition.hasDays()) {
      builder.setDays(transition.getDays());
    }
    if (transition.hasDate()) {
      builder.setDate(transition.getDate());
    }
    return builder.build();
  }

  /** Builder for {@link OmLCTransition}. */
  public static class Builder {
    private Integer days;
    private String date;
    private String storageClass;

    public Builder setDays(int value) {
      days = value;
      return this;
    }

    public Builder setDate(String value) {
      date = value;
      return this;
    }

    public Builder setStorageClass(String value) {
      storageClass = value;
      return this;
    }

    public OmLCTransition build() {
      return new OmLCTransition(this);
    }
  }
}
