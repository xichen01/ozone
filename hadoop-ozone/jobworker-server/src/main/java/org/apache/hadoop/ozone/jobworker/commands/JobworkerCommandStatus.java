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
 *
 */

package org.apache.hadoop.ozone.jobworker.commands;

import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * This class represents the status of a command executed by the jobworker.
 */
public class JobworkerCommandStatus {
  private final long cmdId;
  private OMJobworkerCommandProto.Type type;
  private Status status;
  private String message;

  /**
   * Constructs a JobworkerCommandStatus.
   *
   * @param cmdId   command ID
   * @param type    command type
   * @param status  status enum
   * @param message optional status message
   */
  public JobworkerCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
                                Status status, String message) {
    this.cmdId = cmdId;
    this.type = type;
    this.status = status;
    this.message = message;
  }

  /**
   * Create a new builder.
   *
   * @return Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the command ID.
   *
   * @return command ID
   */
  public long getCmdId() {
    return cmdId;
  }

  /**
   * Returns the command type.
   *
   * @return command type
   */
  public OMJobworkerCommandProto.Type getType() {
    return type;
  }

  /**
   * Sets command type.
   *
   * @param type command type
   */
  public void setType(OMJobworkerCommandProto.Type type) {
    this.type = type;
  }

  /**
   * Returns the status.
   *
   * @return status enum
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Sets the status.
   *
   * @param status status enum
   */
  public void setStatus(Status status) {
    this.status = status;
  }

  /**
   * Returns the message.
   *
   * @return message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Sets the message.
   *
   * @param message message
   */
  public void setMessage(String message) {
    this.message = message;
  }

  /**
   * Converts to protobuf message.
   *
   * @return CommandStatus protobuf message
   */
  public CommandStatus getProtobufMessage() {
    CommandStatus.Builder builder = CommandStatus.newBuilder()
        .setCmdId(cmdId)
        .setStatus(status);

    if (message != null) {
      builder.setMsg(message);
    }

    return builder.build();
  }

  /**
   * Builder for JobworkerCommandStatus.
   */
  public static class Builder {
    private long cmdId;
    private OMJobworkerCommandProto.Type type;
    private Status status;
    private String message;

    /**
     * Sets command ID.
     *
     * @param cmdId command ID
     * @return Builder
     */
    public Builder setCmdId(long cmdId) {
      this.cmdId = cmdId;
      return this;
    }

    /**
     * Sets command type.
     *
     * @param type command type
     * @return Builder
     */
    public Builder setType(OMJobworkerCommandProto.Type type) {
      this.type = type;
      return this;
    }

    /**
     * Sets status.
     *
     * @param status status enum
     * @return Builder
     */
    public Builder setStatus(Status status) {
      this.status = status;
      return this;
    }

    /**
     * Sets message.
     *
     * @param message message
     * @return Builder
     */
    public Builder setMsg(String message) {
      this.message = message;
      return this;
    }

    /**
     * Build the JobworkerCommandStatus.
     *
     * @return JobworkerCommandStatus instance
     */
    public JobworkerCommandStatus build() {
      return new JobworkerCommandStatus(cmdId, type, status, message);
    }
  }
}