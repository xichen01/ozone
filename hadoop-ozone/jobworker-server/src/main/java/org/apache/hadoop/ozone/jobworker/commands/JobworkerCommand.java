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

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;

/**
 * Base class for all jobworker commands.
 *
 * @param <T> Type of the command specific proto inside the wrapper command
 */
public abstract class JobworkerCommand<T extends Message> {
  private final long id;
  private long term;
  private String omServiceId;
  private String encodedToken;
  private long deadline;

  /**
   * Constructor with command ID.
   *
   * @param id Command ID
   */
  public JobworkerCommand(long id) {
    this.id = id;
  }

  /**
   * Returns the command ID.
   *
   * @return ID
   */
  public long getId() {
    return id;
  }

  /**
   * Returns the command type.
   *
   * @return command type
   */
  public abstract OMJobworkerCommandProto.Type getType();

  /**
   * Returns the wrapped proto message.
   *
   * @return proto message
   */
  public abstract T getProto();

  /**
   * Returns the term of the OM leader that generated this command.
   *
   * @return term
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the term of the OM leader.
   *
   * @param term OM leader term
   */
  public void setTerm(long term) {
    this.term = term;
  }

  /**
   * Returns the OM service ID this command was sent from.
   *
   * @return OM service ID
   */
  public String getOmServiceId() {
    return omServiceId;
  }

  /**
   * Sets the OM service ID.
   *
   * @param omServiceId OM service ID
   */
  public void setOmServiceId(String omServiceId) {
    this.omServiceId = omServiceId;
  }

  /**
   * Returns the encoded security token if any.
   *
   * @return encoded token
   */
  public String getEncodedToken() {
    return encodedToken;
  }

  /**
   * Sets the encoded security token.
   *
   * @param encodedToken security token
   */
  public void setEncodedToken(String encodedToken) {
    this.encodedToken = encodedToken;
  }

  /**
   * Returns the deadline for this command.
   *
   * @return deadline timestamp
   */
  public final long getDeadline() {
    return deadline;
  }

  /**
   * Sets the deadline for command execution.
   *
   * @param deadline timestamp
   */
  public final void setDeadline(long deadline) {
    this.deadline = deadline;
  }

  /**
   * Check if the command has expired.
   *
   * @param currentTimeMillis current time in milliseconds
   * @return true if expired, false otherwise
   */
  public boolean hasExpired(long currentTimeMillis) {
    return getDeadline() > 0 && currentTimeMillis > getDeadline();
  }
}
