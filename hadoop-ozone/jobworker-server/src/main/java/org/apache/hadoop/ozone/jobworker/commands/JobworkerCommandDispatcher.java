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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches command to the correct handler.
 */
public final class JobworkerCommandDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerCommandDispatcher.class);

  private final JobworkerStateContext context;
  private final Map<OMJobworkerCommandProto.Type, JobworkerCommandHandler> handlerMap;
  private final JobworkerConnectionManager connectionManager;
  // TODO: Add metrics for command handlers
  // private final JobworkerCommandHandlerMetrics commandHandlerMetrics;

  /**
   * Constructs a command dispatcher.
   *
   * @param context           - JobworkerStateContext
   * @param connectionManager - Connection Manager
   * @param handlers          - Set of handlers
   */
  private JobworkerCommandDispatcher(
      JobworkerConnectionManager connectionManager,
      JobworkerStateContext context,
      JobworkerCommandHandler... handlers) {

    Preconditions.checkNotNull(context, "Context cannot be null");
    Preconditions.checkNotNull(handlers, "Handlers cannot be null");
    Preconditions.checkNotNull(connectionManager, "Connection manager cannot be null");

    this.context = context;
    this.connectionManager = connectionManager;
    handlerMap = new HashMap<>();

    for (JobworkerCommandHandler h : handlers) {
      if (handlerMap.containsKey(h.getCommandType())) {
        LOG.error("Duplicate handler for the same command. Exiting. Handler " +
            "type : {}", h.getCommandType().name());
        throw new IllegalArgumentException("Duplicate handler for the same " +
            "command.");
      }
      handlerMap.put(h.getCommandType(), h);
    }
  }

  /**
   * Creates a new builder for JobworkerCommandDispatcher.
   *
   * @return new Builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Gets a specific command handler by type.
   *
   * @param type command type
   * @return the handler for that command type
   */
  @VisibleForTesting
  public JobworkerCommandHandler getHandler(OMJobworkerCommandProto.Type type) {
    return handlerMap.get(type);
  }

  /**
   * Dispatch the command to the correct handler.
   *
   * @param command - Jobworker Command
   */
  public void handle(JobworkerCommand<?> command) {
    if (command == null) {
      LOG.error("Command cannot be null");
      return;
    }
    if (command.getType() == null) {
      LOG.error("Command type cannot be null");
      return;
    }
    JobworkerCommandHandler handler = handlerMap.get(command.getType());

    if (handler != null) {
      try {
        handler.handle(command, context, connectionManager);
      } catch (Exception ex) {
        LOG.error("Exception while handling command {}: {}",
            command.getType().name(), ex.getMessage(), ex);
      }
    } else {
      LOG.error("Unknown Jobworker Command queued. There is no handler for " +
          "command type: {}", command.getType().name());
    }
  }

  /**
   * Stop all command handlers.
   */
  public void stop() {
    for (JobworkerCommandHandler c : handlerMap.values()) {
      try {
        c.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping command handler for {}: {}",
            c.getCommandType().name(), e.getMessage(), e);
      }
    }

    // If we had metrics: commandHandlerMetrics.unRegister();
  }

  /**
   * For each registered handler, call its getQueuedCount method to retrieve the
   * number of queued commands. The returned map will contain an entry for every
   * registered command in the dispatcher, with a value of zero if there are no
   * queued commands.
   *
   * @return A Map of CommandType where the value is the queued command count.
   */
  public Map<OMJobworkerCommandProto.Type, Integer> getQueuedCommandCount() {
    Map<OMJobworkerCommandProto.Type, Integer> counts = new HashMap<>();
    for (Map.Entry<OMJobworkerCommandProto.Type, JobworkerCommandHandler> entry :
        handlerMap.entrySet()) {
      counts.put(entry.getKey(), entry.getValue().getQueuedCount());
    }
    return counts;
  }

  /**
   * Get a summary of command handlers including invocation counts.
   *
   * @return map of command type to invocation count
   */
  public Map<OMJobworkerCommandProto.Type, Integer> getCommandHandlerSummary() {
    Map<OMJobworkerCommandProto.Type, Integer> summary = new HashMap<>();
    for (Map.Entry<OMJobworkerCommandProto.Type, JobworkerCommandHandler> entry :
        handlerMap.entrySet()) {
      summary.put(entry.getKey(), entry.getValue().getInvocationCount());
    }
    return summary;
  }

  /**
   * Helper class to construct command dispatcher.
   */
  public static class Builder {
    private final List<JobworkerCommandHandler> handlerList;
    private JobworkerStateContext context;
    private JobworkerConnectionManager connectionManager;

    /**
     * Creates a new Builder instance.
     */
    public Builder() {
      handlerList = new LinkedList<>();
    }

    /**
     * Adds a handler.
     *
     * @param handler - handler
     * @return Builder
     */
    public Builder addHandler(JobworkerCommandHandler handler) {
      Preconditions.checkNotNull(handler, "Handler cannot be null");
      handlerList.add(handler);
      return this;
    }

    /**
     * Set the Connection Manager.
     *
     * @param jwConnectionManager connection manager
     * @return this
     */
    public Builder setConnectionManager(JobworkerConnectionManager
                                            jwConnectionManager) {
      Preconditions.checkNotNull(jwConnectionManager,
          "Connection manager cannot be null");
      this.connectionManager = jwConnectionManager;
      return this;
    }

    /**
     * Sets the Context.
     *
     * @param stateContext - JobworkerStateContext
     * @return this
     */
    public Builder setContext(JobworkerStateContext stateContext) {
      Preconditions.checkNotNull(stateContext, "Context cannot be null");
      this.context = stateContext;
      return this;
    }

    /**
     * Builds a command Dispatcher.
     *
     * @return Command Dispatcher.
     */
    public JobworkerCommandDispatcher build() {
      Preconditions.checkNotNull(this.connectionManager,
          "Missing jobworker connection manager.");
      Preconditions.checkNotNull(this.context, "Missing state context.");

      return new JobworkerCommandDispatcher(
          this.connectionManager,
          this.context,
          handlerList.toArray(new JobworkerCommandHandler[0]));
    }
  }
}
