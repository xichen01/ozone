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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for JobworkerCommandHandler implementations.
 */
public abstract class AbstractJobworkerCommandHandler implements JobworkerCommandHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJobworkerCommandHandler.class);

  private final AtomicInteger invocationCount = new AtomicInteger(0);
  private final AtomicLong totalTime = new AtomicLong(0);
  private final AtomicInteger queuedCount = new AtomicInteger(0);
  private final OMJobworkerCommandProto.Type type;

  /**
   * Create a new command handler for the specified type.
   *
   * @param type The command type this handler processes
   */
  protected AbstractJobworkerCommandHandler(OMJobworkerCommandProto.Type type) {
    this.type = type;
  }

  @Override
  public final void handle(JobworkerCommand<?> command,
                           JobworkerStateContext context,
                           JobworkerConnectionManager connectionManager) {
    if (command.getType() != getCommandType()) {
      LOG.warn("Command type mismatch: got {}, expected {}",
          command.getType(), getCommandType());
      return;
    }

    if (command.hasExpired(System.currentTimeMillis())) {
      LOG.warn("Command {} has expired and will not be processed", command.getId());
      updateCommandStatus(context, command, status -> {
        status.setStatus(CommandStatus.Status.FAILED);
        status.setMessage("Command expired before processing");
      }, LOG);
      return;
    }

    long startTime = System.currentTimeMillis();
    invocationCount.incrementAndGet();
    queuedCount.incrementAndGet();
    try {
      processCommand(command, context, connectionManager);
    } catch (Exception e) {
      LOG.error("Error processing command {} of type {}",
          command.getId(), command.getType(), e);
      updateCommandStatus(context, command, status -> {
        status.setStatus(CommandStatus.Status.FAILED);
        status.setMessage("Error: " + e.getMessage());
      }, LOG);
    } finally {
      long endTime = System.currentTimeMillis();
      totalTime.addAndGet(endTime - startTime);
      queuedCount.decrementAndGet();
    }
  }

  /**
   * Process a command. This is the method that subclasses should implement
   * to handle their specific command types.
   *
   * @param command           The command to process
   * @param context           The state context
   * @param connectionManager The connection manager
   * @throws Exception if an error occurs processing the command
   */
  protected abstract void processCommand(JobworkerCommand<?> command,
                                         JobworkerStateContext context,
                                         JobworkerConnectionManager connectionManager)
      throws Exception;

  @Override
  public OMJobworkerCommandProto.Type getCommandType() {
    return type;
  }

  @Override
  public int getInvocationCount() {
    return invocationCount.get();
  }

  @Override
  public long getAverageRunTime() {
    int count = invocationCount.get();
    return count > 0 ? totalTime.get() / count : 0;
  }

  @Override
  public long getTotalRunTime() {
    return totalTime.get();
  }

  @Override
  public int getQueuedCount() {
    return queuedCount.get();
  }

  @VisibleForTesting
  public void setQueuedCount(int queuedCount) {
    this.queuedCount.set(queuedCount);
  }
}
