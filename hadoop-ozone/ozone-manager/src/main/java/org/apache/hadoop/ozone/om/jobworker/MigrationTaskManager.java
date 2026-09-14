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
 */

package org.apache.hadoop.ozone.om.jobworker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.MigrationKeyDBUpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to read and update migration tasks and transactions via OM HA.
 *
 * - Write operations are executed through {@link MigrationKeyDBUpdateManager}.
 * - Read/list operations are lock-free and rely on iterator snapshot semantics;
 *   results are eventually consistent.
 */
public class MigrationTaskManager {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationTaskManager.class);

  private final Table<String, JobworkerMigrationKeysTaskProto> taskTable;
  private final Table<String, JobworkerMigrationKeysTxProto> transactionTable;
  private final MigrationKeyDBUpdateManager dbUpdateManager;

  /**
   * Generates a migration task key from volume, bucket and taskId.
   * @param volume volume name
   * @param bucket bucket name
   * @param taskId task ID
   * @return task key in format "volume/bucket/taskId"
   */
  public static String generateTaskKey(String volume, String bucket, long taskId) {
    return String.join("/", volume, bucket, String.valueOf(taskId));
  }

  /**
   * Generates the prefix for task keys of a volume/bucket.
   * @param volume volume name
   * @param bucket bucket name
   * @return prefix string "volume/bucket/"
   */
  public static String generateTaskKeyPrefix(String volume, String bucket) {
    return String.join("/", volume, bucket) + "/";
  }

  /**
   * Constructs a transaction key from task key and transaction ID.
   * @param taskKey The task key
   * @param txId The transaction ID
   * @return The transaction key in format "taskKey/txId"
   */
  public static String getTransactionKey(String taskKey, long txId) {
    return String.join("/", taskKey, String.valueOf(txId));
  }

  /**
   * Extracts the task key prefix from a transaction key.
   * @param transactionKey transaction key "taskKey/txId"
   * @return taskKey portion of the given transactionKey
   */
  public static String extractTaskKeyFromTransactionKey(String transactionKey) throws IOException {
    int lastSlashIndex = transactionKey.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      throw new IOException("Invalid transaction key format: " + transactionKey);
    }
    return transactionKey.substring(0, lastSlashIndex);
  }

  public MigrationTaskManager(OMMetadataManager metadataManager, OzoneManager ozoneManager, OzoneConfiguration conf) {
    this.taskTable = metadataManager.getJobworkerMigrationKeysTaskTable();
    this.transactionTable = metadataManager.getJobworkerMigrationKeysTxTable();
    this.dbUpdateManager = new MigrationKeyDBUpdateManager(ozoneManager);
  }

  /**
   * Checks if a task exists.
   */
  public boolean isTaskExists(String taskKey) throws IOException {
    return taskTable.isExist(taskKey);
  }

  /**
   * Checks if a transaction exists.
   */
  public boolean isTransactionExists(String transactionKey) throws IOException {
    return transactionTable.isExist(transactionKey);
  }

  /**
   * Gets the task record for a given key, or null if not found.
   */
  public JobworkerMigrationKeysTaskProto getTask(String taskKey) throws IOException {
    return taskTable.get(taskKey);
  }

  /**
   * Requests OM to update the task status.
   */
  public void updateTaskStatus(String taskKey, JobworkerTaskStatus newStatus) throws IOException {
    dbUpdateManager.keyMigrationUpdateTaskStatus(taskKey, newStatus);
  }

  /**
   * Requests OM to complete a transaction: remove the tx and update task counts.
   */
  public void completeTransaction(String taskKey, long txId, int failedKeyCount) throws IOException {
    dbUpdateManager.keyMigrationCompleteTransaction(taskKey, txId, failedKeyCount);
  }

  /**
   * Marks scanning as completed if the task exists; otherwise no-op.
   */
  public void markScanningCompletedIfPresent(String taskKey) throws IOException {
    dbUpdateManager.keyMigrationMarkScanningCompleted(taskKey);
  }

  /**
   * Creates a new task if it does not already exist.
   * Returns true if created, false if it already existed (race-safe).
   */
  public boolean createTaskIfAbsent(String taskKey, String ruleId, ECReplicationConfig ecReplicationConfig)
      throws IOException {
    // Fast-path: if already exists, return false
    if (taskTable.isExist(taskKey)) {
      return false;
    }
    dbUpdateManager.keyMigrationCreateTask(taskKey, ruleId, ecReplicationConfig);
    return true;
  }

  /**
   * Adds a transaction to the task. OM validates existence and duplicates.
   */
  public void addNewTransaction(String taskKey, JobworkerMigrationKeysTxProto migrationKeysTxProto)
      throws IOException {
    dbUpdateManager.keyMigrationAddTransaction(taskKey,
        migrationKeysTxProto.getTxId(), migrationKeysTxProto);
  }

  /**
   * Requests OM to delete a task and its metadata.
   */
  public void cleanupTask(String taskKey) throws IOException {
    dbUpdateManager.keyMigrationCleanupTask(taskKey);
  }

  public void cancelTask(String taskKey) throws IOException {
    if (taskKey == null || taskKey.trim().isEmpty()) {
      throw new IllegalArgumentException("Task key cannot be null or empty");
    }
    JobworkerMigrationKeysTaskProto currentTask = getTask(taskKey);
    if (currentTask == null) {
      throw new IOException("Migration task not found: " + taskKey);
    }
    if (isTaskFinal(currentTask)) {
      throw new IOException("Migration task is already final: " + taskKey);
    }
    dbUpdateManager.cancelMigrationTask(taskKey);
  }

  public void deleteTransactions(String taskKey, List<String> transactionKeys)
      throws IOException {
    if (taskKey == null || taskKey.trim().isEmpty()) {
      throw new IllegalArgumentException("Task key cannot be null or empty");
    }
    dbUpdateManager.deleteTransactions(taskKey, transactionKeys);
  }

  /**
   * Lists all transactions across all tasks.
   */
  public List<TransactionEntry> getAllTransactions() throws IOException {
    return listTransactionsFor("", null, Integer.MAX_VALUE);
  }

  /**
   * Lists transactions for a specific task with pagination support.
   * @param taskKey The task key to list transactions for
   * @param startTransactionKey Key from which listing needs to start. If null, starts from the beginning.
   * @param maxTransactions Maximum number of transactions to return.
   * @return List of transactions. If the returned size equals maxTransactions, there may be more results available.
   */
  public List<TransactionEntry> listTransactionsForTask(String taskKey, String startTransactionKey, 
                                                       int maxTransactions) throws IOException {
    if (StringUtils.isEmpty(taskKey)) {
      throw new IOException("Task key cannot be null or empty");
    }
    if (maxTransactions <= 0) {
      return new ArrayList<>();
    }
    if (!taskKey.endsWith("/")) {
      taskKey = taskKey + "/";
    }
    return listTransactionsFor(taskKey, startTransactionKey, maxTransactions);
  }

  private List<TransactionEntry> listTransactionsFor(String taskKey, String startTransactionKey,
      int maxTransactions) throws IOException {
    if (maxTransactions <= 0) {
      return new ArrayList<>();
    }
    TreeMap<String, TransactionEntry> cacheTransactionMap = new TreeMap<>();
    HashSet<String> deletedKeys = new HashSet<>();
    String seekKey = taskKey;
    if (StringUtils.isNotEmpty(startTransactionKey)) {
      // If startTransactionKey is provided, start from there
      seekKey = startTransactionKey;
    }
    boolean hasStartKey = StringUtils.isNotEmpty(startTransactionKey);

    Iterator<Entry<CacheKey<String>, CacheValue<JobworkerMigrationKeysTxProto>>> cacheIterator =
        transactionTable.cacheIterator();
    while (cacheTransactionMap.size() < maxTransactions && cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<JobworkerMigrationKeysTxProto>> cacheEntry =
          cacheIterator.next();
      String transactionKey = cacheEntry.getKey().getCacheKey();
      CacheValue<JobworkerMigrationKeysTxProto> cacheValue = cacheEntry.getValue();
      if (transactionKey.startsWith(taskKey) && 
          (!hasStartKey || transactionKey.compareTo(startTransactionKey) >= 0)) {
        if (hasStartKey && transactionKey.equals(startTransactionKey)) {
          continue; // Skip the start key itself
        }
        JobworkerMigrationKeysTxProto transaction = cacheValue.getCacheValue();
        if (transaction != null) {
          // Entry exists in cache
          cacheTransactionMap.put(transactionKey, new TransactionEntry(transactionKey, transaction));
        } else {
          deletedKeys.add(transactionKey);
        }
      }
    }

    List<TransactionEntry> resultTransactions = new ArrayList<>(cacheTransactionMap.values());

    // Only access DB if we haven't reached the limit from cache
    if (resultTransactions.size() < maxTransactions) {
      try (TableIterator<String, ? extends KeyValue<String, JobworkerMigrationKeysTxProto>>
               iterator = transactionTable.iterator()) {
        iterator.seek(seekKey);
        while (iterator.hasNext()) {
          Table.KeyValue<String, JobworkerMigrationKeysTxProto> entry = iterator.next();
          String transactionKey = entry.getKey();
          // Check if this transaction belongs to our task
          if (!transactionKey.startsWith(taskKey)) {
            break; // No more transactions for this task
          }
          // Skip if already processed from cache
          if (cacheTransactionMap.containsKey(transactionKey)) {
            continue;
          }
          if (deletedKeys.contains(transactionKey)) {
            continue;
          }
          // Skip the start key itself
          if (hasStartKey && transactionKey.equals(startTransactionKey)) {
            continue;
          }
          
          resultTransactions.add(new TransactionEntry(transactionKey, entry.getValue()));
        }
      }
    }
    return resultTransactions;
  }

  /**
   * Lists all migration tasks. Results are eventually consistent.
   */
  public List<TaskEntry> getAllTasks() throws IOException {
    return listTask("");
  }

  /**
   * Lists all tasks for a specific volume and bucket. Results are eventually consistent.
   */
  public List<TaskEntry> listTask(String volume, String bucket) throws IOException {
    return listTask(generateTaskKeyPrefix(volume, bucket));
  }

  /**
   * Checks whether a non-final task already exists for the lifecycle rule.
   */
  public boolean hasActiveTask(String volume, String bucket, String ruleId,
      ECReplicationConfig ecReplicationConfig) throws IOException {
    for (TaskEntry entry : listTask(volume, bucket)) {
      JobworkerMigrationKeysTaskProto task = entry.getTask();
      if (task == null || !ruleId.equals(task.getRuleId()) ||
          !ecReplicationConfig.toProto().equals(task.getEcReplicationConfig())) {
        continue;
      }
      if (!isTaskFinal(task)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isTaskFinal(JobworkerMigrationKeysTaskProto task) {
    JobworkerTaskStatus status = task.getMigrationStatus();
    return status == JobworkerTaskStatus.COMPLETED ||
        status == JobworkerTaskStatus.FAILED ||
        status == JobworkerTaskStatus.FAILING ||
        status == JobworkerTaskStatus.CANCELED ||
        status == JobworkerTaskStatus.CANCELING;
  }

  private List<TaskEntry> listTask(String taskKeyPrefix) throws IOException {
    TreeMap<String, TaskEntry> taskMap = new TreeMap<>();

    // First, find tasks in table cache
    Iterator<Map.Entry<CacheKey<String>, CacheValue<JobworkerMigrationKeysTaskProto>>>
        cacheIterator = taskTable.cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<JobworkerMigrationKeysTaskProto>> cacheEntry =
          cacheIterator.next();
      String taskKey = cacheEntry.getKey().getCacheKey();
      CacheValue<JobworkerMigrationKeysTaskProto> cacheValue = cacheEntry.getValue();
      if (taskKey.startsWith(taskKeyPrefix)) {
        JobworkerMigrationKeysTaskProto task = cacheValue.getCacheValue();
        if (task != null) {
          // Entry exists in cache
          taskMap.put(taskKey, new TaskEntry(taskKey, task));
        } else {
          // Entry is deleted in cache, mark for exclusion
          taskMap.put(taskKey, null);
        }
      }
    }

    // Then, find tasks in DB
    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTaskProto>>
             iterator = taskTable.iterator()) {
      iterator.seek(taskKeyPrefix);
      while (iterator.hasNext()) {
        Table.KeyValue<String, JobworkerMigrationKeysTaskProto> entry = iterator.next();
        String taskKey = entry.getKey();
        if (!taskKey.startsWith(taskKeyPrefix)) {
          break; // No more tasks for this volume/bucket
        }
        // Only add from DB if not already processed from cache
        if (!taskMap.containsKey(taskKey)) {
          taskMap.put(taskKey, new TaskEntry(taskKey, entry.getValue()));
        }
      }
    }

    List<TaskEntry> tasks = new ArrayList<>();
    for (TaskEntry entry : taskMap.values()) {
      if (entry != null) { // Skip deleted entries
        tasks.add(entry);
      }
    }

    return tasks;
  }

  /**
   * Simple data class to hold task information.
   */
  public static class TaskEntry {
    private final String taskKey;
    private final JobworkerMigrationKeysTaskProto task;

    public TaskEntry(String taskKey, JobworkerMigrationKeysTaskProto task) {
      this.taskKey = taskKey;
      this.task = task;
    }

    public String getTaskKey() {
      return taskKey;
    }

    public JobworkerMigrationKeysTaskProto getTask() {
      return task;
    }
  }

  /**
   * Simple data class to hold transaction information.
   */
  public static class TransactionEntry {
    private final String transactionKey;
    private final JobworkerMigrationKeysTxProto transaction;

    public TransactionEntry(String transactionKey, JobworkerMigrationKeysTxProto transaction) {
      this.transactionKey = transactionKey;
      this.transaction = transaction;
    }

    public String getTransactionKey() {
      return transactionKey;
    }

    public JobworkerMigrationKeysTxProto getTransaction() {
      return transaction;
    }
  }
}
