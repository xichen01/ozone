/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.jobworker.commands;

import com.google.common.base.Preconditions;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * A class that is used to communicate status of jobworker commands.
 */
public class JobworkerCommandStatus {

  private final long cmdId;
  private final OMJobworkerCommandProto.Type type;
  private volatile Status status;
  private final String omServiceId;
  private volatile String message;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Constructs a JobworkerCommandStatus.
   *
   * @param cmdId   command ID
   * @param type    command type
   * @param status  status enum
   * @param message optional status message
   */
  public JobworkerCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
                                Status status, String omServiceId, String message) {
    Preconditions.checkArgument(cmdId >= 0, "CmdId must be >= 0");
    Preconditions.checkNotNull(type, "Type must be not null");
    Preconditions.checkNotNull(status, "Status must be not null");
    Preconditions.checkNotNull(omServiceId, "OmServiceId must be not null");
    this.cmdId = cmdId;
    this.type = type;
    this.status = status;
    this.omServiceId = omServiceId;
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
   * Returns the status.
   *
   * @return status enum
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Check if this command is in a terminal state (SUCCEEDED or FAILED).
   *
   * @return true if the command is in a terminal state
   */
  public boolean isTerminalState() {
    Status currentStatus = status; // Capture once for consistency
    return currentStatus == CommandStatus.Status.SUCCEEDED ||
        currentStatus == CommandStatus.Status.FAILED;
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
   * Atomically updates both status and message to ensure they are seen together
   * by other threads.
   *
   * @param newStatus new status
   * @param msg new message
   */
  public void updateStatusAndMessage(Status newStatus, String msg) {
    Preconditions.checkNotNull(newStatus, "New status must be not null");
    lock.writeLock().lock();
    try {
      this.status = newStatus;
      this.message = msg;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Converts to a protobuf message.
   *
   * @return CommandStatus protobuf message
   */
  public CommandStatus getProtobufMessage() {
    lock.readLock().lock();
    try {
      CommandStatus.Builder builder = CommandStatus.newBuilder()
          .setCmdId(cmdId)
          .setStatus(status)
          .setType(type)
          .setOmServiceId(omServiceId);

      if (message != null) {
        builder.setMsg(message);
      }

      return builder.build();
    } finally {
      lock.readLock().unlock();
    }
  }

  public String getOmServiceId() {
    return omServiceId;
  }

  /**
   * Builder for JobworkerCommandStatus.
   */
  public static class Builder {
    private long cmdId;
    private OMJobworkerCommandProto.Type type;
    private Status status;
    private String omServiceId;
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
     * @param msg message
     * @return Builder
     */
    public Builder setMsg(String msg) {
      this.message = msg;
      return this;
    }

    /**
     * Sets omServiceId.
     * @param omServiceId, the command's omServiceId
     * @return Builder
     */
    public Builder setOmServiceId(String omServiceId) {
      this.omServiceId = omServiceId;
      return this;
    }

    /**
     * Build the JobworkerCommandStatus.
     *
     * @return JobworkerCommandStatus instance
     */
    public JobworkerCommandStatus build() {
      return new JobworkerCommandStatus(cmdId, type, status, omServiceId, message);
    }
  }
}
