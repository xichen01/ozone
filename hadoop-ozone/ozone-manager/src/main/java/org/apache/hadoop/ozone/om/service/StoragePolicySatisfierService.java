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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerMigrateKeyCommand;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerInfo;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StoragePolicySatisfierService is responsible for scanning keys that require
 * storage policy migration and dispatching them to Jobworkers for execution.
 */
public class StoragePolicySatisfierService extends BackgroundService {
  private static final Logger LOG = LoggerFactory.getLogger(StoragePolicySatisfierService.class);

  private final OzoneManager ozoneManager;
  private final OMMetadataManager metadataManager;
  private final OMJobworkerCommandManager commandManager;
  private final JobworkerNodeManager nodeManager;
  private final JobWorkerMigrationKeyConfiguration config;
  private final MigrationTaskManager taskManager;

  private final Map<String, CompletableFuture<JobworkerTaskStatus>> runningMigrations = new ConcurrentHashMap<>();
  private int totalActivatedTaskCount = 0;

  private final long incompleteTaskTimeoutMs;
  private final long taskRetentionTimeoutMs;
  private final int maxConcurrentMigrationTasks;
  private final int maxInflightCommandCount;
  private final long maxTaskWaitingTimeMs;


  /**
   * Constructs a new StoragePolicySatisfierService.
   *
   * @param intervalMs service execution interval in milliseconds
   * @param serviceTimeoutMs service timeout in milliseconds
   * @param ozoneManager the OzoneManager instance
   * @param configuration the Ozone configuration
   */
  public StoragePolicySatisfierService(long intervalMs, long serviceTimeoutMs,
      OzoneManager ozoneManager, OzoneConfiguration configuration) {
    super(StoragePolicySatisfierService.class.getSimpleName(), intervalMs, TimeUnit.MILLISECONDS,
        1, serviceTimeoutMs);
    this.ozoneManager = ozoneManager;
    this.metadataManager = ozoneManager.getMetadataManager();
    this.commandManager = ozoneManager.getOMJobworkerCommandManager();
    this.nodeManager = ozoneManager.getJobworkerNodemanager();
    this.config = configuration.getObject(JobWorkerMigrationKeyConfiguration.class);
    this.taskManager = ozoneManager.getMigrationTaskManager();

    this.incompleteTaskTimeoutMs = config.getIncompleteTaskTimeoutMs();
    this.taskRetentionTimeoutMs = config.getCompletedTaskRetentionTimeMs();
    this.maxConcurrentMigrationTasks = config.getMaxConcurrentTasks();
    this.maxInflightCommandCount = config.getMaxInflightCommandCount();
    this.maxTaskWaitingTimeMs = config.getMaxTaskWaitingTimeMs();
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new StoragePolicySatisfierTask());
    return queue;
  }

  void processStoragePolicyMigration() {
    try {
      if (!ozoneManager.isLeaderReady()) {
        LOG.debug("OM is not leader ready, skipping migration processing");
        return;
      }

      LOG.info("Starting storage policy migration task processing");

      Table<String, JobworkerMigrationKeysTaskProto> statusTable =
          metadataManager.getJobworkerMigrationKeysTaskTable();

      // Update incomplete tasks and cleanup completed tasks
      processExistingTasks(statusTable);
      // Check and update running migration statuses
      updateRunningMigrationStatuses();
      // Start new migration tasks if capacity allows
      startNewMigrationTasks(statusTable);
      LOG.debug("Completed storage policy migration scan. Active tasks: {}", getRunningTaskCount());
    } catch (IOException e) {
      LOG.error("Error scanning migration status table", e);
    }
  }

  private void processExistingTasks(Table<String, JobworkerMigrationKeysTaskProto> statusTable)
      throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTaskProto>> iterator =
             statusTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, JobworkerMigrationKeysTaskProto> entry = iterator.next();
        String taskKey = entry.getKey();
        JobworkerMigrationKeysTaskProto task = entry.getValue();

        handleIncompleteTaskTimeout(taskKey, task);
        cleanupCompletedTask(taskKey, task);
      }
    }
  }

  private void startNewMigrationTasks(Table<String, JobworkerMigrationKeysTaskProto> statusTable)
      throws IOException {
    List<JobworkerInfo> healthyJobworkers = nodeManager.getNodeStateManager().getHealthyJobworkerInfos();
    if (healthyJobworkers.isEmpty()) {
      LOG.warn("No healthy Jobworkers available for migration tasks");
      return;
    }

    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTaskProto>> iterator =
             statusTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, JobworkerMigrationKeysTaskProto> entry = iterator.next();
        String taskKey = entry.getKey();
        JobworkerMigrationKeysTaskProto task = entry.getValue();

        if (runningMigrations.containsKey(taskKey) || isTaskFinished(task)) {
          continue;
        }

        if (getRunningTaskCount() >= maxConcurrentMigrationTasks) {
          LOG.debug("Maximum concurrent migration tasks ({}) reached, skipping new task submission",
              maxConcurrentMigrationTasks);
          break;
        }

        startKeyMigrationTask(taskKey, task);
      }
    }
  }

  private void updateRunningMigrationStatuses() {
    runningMigrations.entrySet().removeIf(entry -> {
      String taskKey = entry.getKey();
      CompletableFuture<JobworkerTaskStatus> future = entry.getValue();

      if (future.isDone()) {
        try {
          JobworkerTaskStatus resultStatus = future.get();
          updateMigrationTaskStatus(taskKey, resultStatus);
        } catch (ExecutionException e) {
          LOG.error("Migration task execution failed for task: {}", taskKey, e.getCause());
          updateMigrationTaskStatus(taskKey, JobworkerTaskStatus.FAILED);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Migration task interrupted for task: {}", taskKey);
          updateMigrationTaskStatus(taskKey, JobworkerTaskStatus.PAUSED);
        }
        return true; // Remove from running migrations
      }
      return false; // Keep in running migrations
    });
  }

  @VisibleForTesting
  public int getRunningTaskCount() {
    return (int) runningMigrations.values().stream()
        .filter(future -> !future.isDone())
        .count();
  }

  private boolean isTaskFinished(JobworkerMigrationKeysTaskProto task) {
    JobworkerTaskStatus taskStatus = task.getMigrationStatus();
    return taskStatus == JobworkerTaskStatus.COMPLETED ||
        taskStatus == JobworkerTaskStatus.CANCELED ||
        taskStatus == JobworkerTaskStatus.FAILED;
  }

  /**
   * Handles timeout for incomplete tasks that are stuck in executing state.
   *
   * @param taskKey the task identifier
   * @param task the migration task
   */
  private void handleIncompleteTaskTimeout(String taskKey, JobworkerMigrationKeysTaskProto task) {
    if (task.getMigrationStatus() == JobworkerTaskStatus.EXECUTING && !task.getCompleteScanning()) {
      long timeSinceUpdate = System.currentTimeMillis() - task.getLastUpdateTime();
      if (timeSinceUpdate > incompleteTaskTimeoutMs) {
        LOG.warn("Task {} has exceeded incomplete timeout ({}ms), canceling", taskKey, incompleteTaskTimeoutMs);
        try {
          taskManager.updateTaskStatus(taskKey, JobworkerTaskStatus.CANCELED);
        } catch (IOException e) {
          LOG.error("Failed to update task status to CANCELED for task: {}", taskKey, e);
        }
      }
    }
  }

  /**
   * Cleans up completed tasks that have exceeded their retention timeout.
   *
   * @param taskKey the task identifier
   * @param task the migration task
   */
  private void cleanupCompletedTask(String taskKey, JobworkerMigrationKeysTaskProto task) {
    if (isTaskFinished(task)) {
      long timeSinceUpdate = System.currentTimeMillis() - task.getLastUpdateTime();
      if (timeSinceUpdate > taskRetentionTimeoutMs) {
        LOG.debug("Cleaning up completed task {} after retention timeout", taskKey);
        try {
          taskManager.cleanupTask(taskKey);
        } catch (IOException e) {
          LOG.error("Failed to cleanup completed task: {}", taskKey, e);
        }
      }
    }
  }

  /**
   * Starts a new key migration task by creating and executing a migration thread.
   *
   * @param taskKey the task identifier
   * @param task the migration task to start
   */
  private void startKeyMigrationTask(String taskKey, JobworkerMigrationKeysTaskProto task) {
    CompletableFuture<JobworkerTaskStatus> future = CompletableFuture.supplyAsync(() -> {
      KeyMigrationThread migrationThread = new KeyMigrationThread(taskKey, task);
      return migrationThread.processMigrationTasks();
    });
    totalActivatedTaskCount++;

    JobworkerTaskStatus currentStatus = task.getMigrationStatus();
    if (currentStatus == JobworkerTaskStatus.PENDING || currentStatus == JobworkerTaskStatus.PAUSED) {
      updateMigrationTaskStatus(taskKey, JobworkerTaskStatus.EXECUTING);
    }

    runningMigrations.put(taskKey, future);
    LOG.info("Started migration thread for the task: {}. Active tasks: {}", taskKey, getRunningTaskCount());
  }

  private void updateMigrationTaskStatus(String taskKey, JobworkerTaskStatus newStatus) {
    try {
      taskManager.updateTaskStatus(taskKey, newStatus);
      LOG.debug("Updated task {} status to {}", taskKey, newStatus);
    } catch (IOException e) {
      LOG.error("Failed to update migration status to {} for task: {}", newStatus, taskKey, e);
    }
  }

  /**
   * Thread responsible for processing migration tasks for a specific bucket.
   */
  private class KeyMigrationThread {
    private final String taskKey;
    private final JobworkerMigrationKeysTaskProto task;
    private final List<Long> sentCommands = new ArrayList<>();
    private final Table<String, JobworkerMigrationKeysTxProto> transactionTable;
    private boolean noAvailableJobworker = false;

    private static final int TRANSACTION_BATCH_SIZE = 100;
    private static final long COMMAND_QUEUE_CHECK_INTERVAL_MS = 2000;
    private static final long TASK_COMPLETION_CHECK_INTERVAL_MS = 1000;

    KeyMigrationThread(String taskKey, JobworkerMigrationKeysTaskProto task) {
      this.taskKey = taskKey;
      this.task = task;
      this.transactionTable = metadataManager.getJobworkerMigrationKeysTxTable();
    }

    private JobworkerTaskStatus processMigrationTasks() {
      try {
        LOG.info("Starting key migration processing for task: {}", taskKey);

        boolean hasMoreTasks = processTransactionTable();
        // Wait for any remaining Commands to complete
        waitForTaskCompletion(sentCommands);
        sentCommands.clear();

        // Determine final status based on scanning completion and remaining tasks
        JobworkerTaskStatus finalStatus;
        if (task.getCompleteScanning() && !hasMoreTasks) {
          finalStatus = JobworkerTaskStatus.COMPLETED;
        } else if (noAvailableJobworker) {
          finalStatus = JobworkerTaskStatus.PAUSED;
        } else {
          finalStatus = JobworkerTaskStatus.EXECUTING;
        }

        LOG.info("Completed key migration processing for task: {} with status: {}",
            taskKey, finalStatus);
        return finalStatus;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Key migration thread interrupted for task: {}", taskKey);
        return JobworkerTaskStatus.FAILED;
      } catch (Exception e) {
        LOG.error("Error in key migration thread for task: {}", taskKey, e);
        return JobworkerTaskStatus.FAILED;
      }
    }

    private boolean processTransactionTable() throws IOException, InterruptedException {
      boolean hasMoreTasks = false;

      try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTxProto>> iterator =
               transactionTable.iterator()) {
        iterator.seek(taskKey);

        while (iterator.hasNext()) {
          Table.KeyValue<String, JobworkerMigrationKeysTxProto> entry = iterator.next();
          String migrationTxKey = entry.getKey();

          // Check if this transaction belongs to our task
          String taskKeyPrefix = extractTaskKeyFromTxKey(migrationTxKey);
          if (!taskKeyPrefix.equals(taskKey)) {
            continue;
          }

          hasMoreTasks = true;

          waitForCommandQueueCapacity();
          JobworkerMigrationKeysTxProto txProto = entry.getValue();
          if (dispatchMigrationCommand(migrationTxKey, txProto)) {
            // Process in batches to avoid memory issues
            if (sentCommands.size() >= TRANSACTION_BATCH_SIZE) {
              waitForTaskCompletion(sentCommands);
              sentCommands.clear();
            }
          } else {
            noAvailableJobworker = true;
            return hasMoreTasks;
          }
        }
      }

      return hasMoreTasks;
    }

    private String extractTaskKeyFromTxKey(String migrationTxKey) {
      return migrationTxKey.substring(0, migrationTxKey.lastIndexOf('/'));
    }

    private void waitForCommandQueueCapacity() throws InterruptedException {
      while (true) {
        int inflightCmdCount = commandManager.getInFlightCommandCount(Type.migrateKeyCommand);
        if (inflightCmdCount <= maxInflightCommandCount) {
          break;
        }
        LOG.debug("In-flight command count ({}) exceeds limit ({}), waiting...",
            inflightCmdCount, maxInflightCommandCount);
        Thread.sleep(COMMAND_QUEUE_CHECK_INTERVAL_MS);
      }
    }

    private boolean dispatchMigrationCommand(String migrationTxKey, JobworkerMigrationKeysTxProto txProto)
        throws IOException {
      List<JobworkerInfo> healthyJobworkers = nodeManager.getNodeStateManager().getHealthyJobworkerInfos();
      if (healthyJobworkers.isEmpty()) {
        LOG.error("No healthy Jobworkers available for task: {}", migrationTxKey);
        return false;
      }

      JobworkerInfo selectedJobworker = selectRandomJobworker(healthyJobworkers);
      long commandId = commandManager.sendCommand(
          selectedJobworker.getUuid(), new OMJobworkerMigrateKeyCommand(txProto, 0));
      sentCommands.add(commandId);

      LOG.debug("Dispatched migration command {} to JobWorker {} for transaction: {}",
          commandId, selectedJobworker.getUuid(), migrationTxKey);
      return true;
    }

    /**
     * Wait for the commands to complete.
     * Note that the completion of a command does not mean that the command was successful.
     * The command may be executed successfully or failed, or be retried, etc.
     */
    private void waitForTaskCompletion(List<Long> commands) throws InterruptedException, IOException {
      if (commands.isEmpty()) {
        return;
      }

      LOG.debug("Waiting for {} Command to complete", commands.size());
      long startTime = System.currentTimeMillis();

      for (long commandId : commands) {
        while (commandManager.isCommandInFlight(Type.migrateKeyCommand, commandId)) {
          if (System.currentTimeMillis() - startTime > maxTaskWaitingTimeMs) {
            LOG.warn("Command completion wait exceeded timeout ({}ms) for Command: {}",
                maxTaskWaitingTimeMs, commandId);
            return;
          }
          LOG.debug("Waiting for Command {} to complete", commandId);
          Thread.sleep(TASK_COMPLETION_CHECK_INTERVAL_MS);
        }
      }
    }

    private JobworkerInfo selectRandomJobworker(List<JobworkerInfo> healthyJobworkers) {
      if (healthyJobworkers.size() == 1) {
        return healthyJobworkers.get(0);
      }

      int randomIndex = ThreadLocalRandom.current().nextInt(healthyJobworkers.size());
      JobworkerInfo selected = healthyJobworkers.get(randomIndex);
      LOG.debug("Selected JobWorker {} from {} available healthy JobWorkers",
          selected.getUuid(), healthyJobworkers.size());
      return selected;
    }
  }

  /**
   * Background task implementation for the StoragePolicySatisfier service.
   */
  private class StoragePolicySatisfierTask implements BackgroundTask {

    @Override
    public BackgroundTaskResult call() {
      try {
        processStoragePolicyMigration();
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      } catch (Exception e) {
        LOG.error("Error during storage policy migration processing", e);
        return BackgroundTaskResult.EmptyTaskResult.newResult();
      }
    }

    @Override
    public int getPriority() {
      return 0;
    }
  }

  @VisibleForTesting
  public Map<String, CompletableFuture<JobworkerTaskStatus>> getRunningMigrations() {
    return runningMigrations;
  }

  @VisibleForTesting
  public int getTotalActivatedTasksCount() {
    return totalActivatedTaskCount;
  }
}

