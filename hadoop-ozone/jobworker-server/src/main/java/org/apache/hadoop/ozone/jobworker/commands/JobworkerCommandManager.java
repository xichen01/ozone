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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerCommandManager is responsible for managing command queue, command status tracking,
 * and leadership term management for the JobWorker component.
 */
public class JobworkerCommandManager {

  private static final Logger LOG = LoggerFactory.getLogger(JobworkerCommandManager.class);

  private final Queue<JobworkerCommand> commandQueue;
  private final Map<String, Map<Long, JobworkerCommandStatus>> cmdStatusMap;
  private final Lock lock;
  private final int maxCommandQueueLimit;

  // OM leadership terms, mapped by service ID
  private final Map<String, OptionalLong> termOfLeaderOM;

  /**
   * Constructs a new JobworkerCommandManager.
   *
   * @param conf Configuration source
   */
  public JobworkerCommandManager(ConfigurationSource conf) {
    this.maxCommandQueueLimit = conf.getObject(JobworkerClientConfiguration.class)
        .getCommandQueueLimit();
    this.commandQueue = new LinkedList<>();
    this.cmdStatusMap = new ConcurrentHashMap<>();
    this.lock = new ReentrantLock();
    this.termOfLeaderOM = new ConcurrentHashMap<>();
  }

  /**
   * Get the lock used for command queue synchronization.
   *
   * @return the lock
   */
  public Lock getLock() {
    return lock;
  }

  /**
   * Returns the command status for a given command ID.
   *
   * @param omServiceId The OM service ID
   * @param key         The command ID
   * @return CommandStatus or null
   */
  public JobworkerCommandStatus getCmdStatus(String omServiceId, Long key) {
    if (cmdStatusMap.get(omServiceId) != null) {
      return cmdStatusMap.get(omServiceId).get(key);
    }
    return null;
  }

  /**
   * Adds a command status for a given command.
   *
   * @param cmd JobworkerCommand
   */
  public void addCmdStatus(JobworkerCommand cmd) {
    String omServiceId = cmd.getOmServiceId();
    JobworkerCommandStatus status = JobworkerCommandStatus.newBuilder()
        .setCmdId(cmd.getId())
        .setType(cmd.getType())
        .setStatus(CommandStatus.Status.PENDING)
        .setOmServiceId(omServiceId)
        .build();
    cmdStatusMap.computeIfAbsent(omServiceId, ignore -> new ConcurrentHashMap<>())
        .put(cmd.getId(), status);
  }

  /**
   * Get map holding all command status objects.
   *
   * @return map of command statuses
   */
  public Map<String, Map<Long, JobworkerCommandStatus>> getCommandStatusMap() {
    return cmdStatusMap;
  }

  /**
   * After startup, jobworker needs to detect latest leader OM for each service
   * before handling any JobworkerCommand, so that it won't be disturbed by
   * stale leader OMs.
   * <p>
   * The rule is: after we have received commands with terms from a service,
   * initialize the termOfLeaderOM for that service with the max term found.
   * <p>
   * This init process also works for non-HA mode. In that case, term of all
   * commands will be 0.
   *
   * @param omServiceId The OM service ID
   */
  private void initTermOfLeaderOM(String omServiceId) {
    // only init once for each service ID
    if (termOfLeaderOM.containsKey(omServiceId)) {
      return;
    }

    // Initialize with OptionalLong.empty() until we get commands
    termOfLeaderOM.putIfAbsent(omServiceId, OptionalLong.empty());

    lock.lock();
    try {
      // Check if we have any commands yet to initialize with a term
      OptionalLong maxTerm = commandQueue.stream()
          .filter(cmd -> omServiceId.equals(cmd.getOmServiceId()))
          .mapToLong(JobworkerCommand::getTerm)
          .max();

      if (maxTerm.isPresent()) {
        termOfLeaderOM.put(omServiceId, maxTerm);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update the term of leader OM for a specific service ID.
   *
   * @param omServiceId The OM service ID
   * @param newTerm     New term value
   */
  public void updateTermOfLeaderOM(String omServiceId, long newTerm) {
    OptionalLong currentTerm = termOfLeaderOM.getOrDefault(
        omServiceId, OptionalLong.empty());

    if (!currentTerm.isPresent() || currentTerm.getAsLong() < newTerm) {
      termOfLeaderOM.put(omServiceId, OptionalLong.of(newTerm));
    }
  }

  public void updateCommand(JobworkerCommand command,
                            CommandStatus.Status status, String message) {
    long commandId = command.getId();
    String omServiceId = command.getOmServiceId();
    Map<Long, JobworkerCommandStatus> commands = cmdStatusMap.get(omServiceId);
    if (commands != null && commands.get(commandId) != null) {
      commands.get(commandId).updateStatusAndMessage(status, message);
    } else {
      LOG.warn("CommandStatus Type {} with ID: {} not found.", command.getType(),
          command.getId());
    }
  }

  public void updateCommand(JobworkerCommand command,
                            Consumer<JobworkerCommandStatus> cmdStatusUpdater) {
    long commandId = command.getId();
    String omServiceId = command.getOmServiceId();
    Map<Long, JobworkerCommandStatus> commands = cmdStatusMap.get(omServiceId);
    if (commands != null && commands.get(commandId) != null) {
      cmdStatusUpdater.accept(commands.get(commandId));
    } else {
      LOG.warn("CommandStatus Type {} with ID: {} not found.", command.getType(),
          command.getId());
    }
  }

  /**
   * Returns the next command or null if queue is empty.
   *
   * @return JobworkerCommand or Null
   */
  public JobworkerCommand getNextCommand() {
    lock.lock();
    try {
      while (true) {
        JobworkerCommand command = commandQueue.poll();
        if (command == null) {
          return null;
        }

        String omServiceId = command.getOmServiceId();
        if (!termOfLeaderOM.containsKey(omServiceId)) {
          initTermOfLeaderOM(omServiceId);
        }

        updateTermOfLeaderOM(omServiceId, command.getTerm());
        OptionalLong currentTerm = termOfLeaderOM.get(omServiceId);
        if (!currentTerm.isPresent()) {
          // updateTermOfLeaderOM will update current Term,
          // so normal business logic cannot reach here
          LOG.error("No Term found for OM service id {}", omServiceId);
          return null;
        }
        if (command.getTerm() == currentTerm.getAsLong()) {
          return command;
        }

        // If we get here, this command is from a stale leader
        LOG.warn("Detect and drop a JobworkerCommand {} from stale leader OM for service {}," +
                " stale term {}, latest term {}.",
            command, omServiceId, command.getTerm(), currentTerm.getAsLong());
        Map<Long, JobworkerCommandStatus> commands = cmdStatusMap.get(command.getOmServiceId());
        if (commands != null &&  commands.get(command.getId()) != null) {
          commands.get(command.getId()).updateStatusAndMessage(CommandStatus.Status.FAILED, "Stale command");
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a command to the command queue.
   *
   * @param command - JobworkerCommand
   */
  public void addCommand(JobworkerCommand command) {
    lock.lock();
    try {
      if (commandQueue.size() >= maxCommandQueueLimit) {
        // TODO jobworker add metrics
        LOG.warn("Ignore command {} as command queue crosses max limit {}.",
            command.getType(), maxCommandQueueLimit);
        // TODO jobworker add metrics set status to failure
        return;
      }

      String omServiceId = command.getOmServiceId();
      if (!termOfLeaderOM.containsKey(omServiceId)) {
        initTermOfLeaderOM(omServiceId);
      }
      updateTermOfLeaderOM(omServiceId, command.getTerm());

      commandQueue.add(command);
      addCmdStatus(command);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gets a summary of commands in the queue by type.
   *
   * @return Map of command types to counts
   */
  public Map<OMJobworkerCommandProto.Type, Integer> getCommandQueueSummary() {
    Map<OMJobworkerCommandProto.Type, Integer> summary = new HashMap<>();
    lock.lock();
    try {
      for (JobworkerCommand cmd : commandQueue) {
        summary.put(cmd.getType(), summary.getOrDefault(cmd.getType(), 0) + 1);
      }
    } finally {
      lock.unlock();
    }
    return summary;
  }

  /**
   * Get the leader Term of the OM Group based on omServiceId.
   *
   * @param omServiceId
   * @return
   */
  public OptionalLong getTermOfLeaderOMByServiceId(String omServiceId) {
    return termOfLeaderOM.getOrDefault(omServiceId, OptionalLong.empty());
  }
}
