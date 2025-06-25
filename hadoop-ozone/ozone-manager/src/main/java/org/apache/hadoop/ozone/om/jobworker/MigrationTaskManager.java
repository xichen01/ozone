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

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Manages migration tasks with striped locks to ensure thread safety.
 * This class provides synchronized access to migration task tables and status.
 */
public class MigrationTaskManager {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationTaskManager.class);
  // TODO Let all write requests go through Ratis for HA transfer compatibility
  private final OMMetadataManager metadataManager;
  private final Striped<ReadWriteLock> stripedLock;
  private final Table<String, JobworkerMigrationKeysTaskProto> taskTable;
  private final Table<String, JobworkerMigrationKeysTxProto> transactionTable;
  private static final int LOCK_STRIPE_SIZE = 512;

  /**
   * Constructs transaction key from task key and transaction ID.
   * @param taskKey The task key
   * @param txId The transaction ID
   * @return The transaction key in format "taskKey/txId"
   */
  public static String getTransactionKey(String taskKey, long txId) {
    return taskKey + "/" + txId;
  }

  public MigrationTaskManager(OMMetadataManager metadataManager, OzoneConfiguration conf) {
    this.metadataManager = metadataManager;
    this.taskTable = metadataManager.getJobworkerMigrationKeysTaskTable();
    this.transactionTable = metadataManager.getJobworkerMigrationKeysTxTable();
    this.stripedLock = Striped.readWriteLock(LOCK_STRIPE_SIZE);
  }

  /**
   * Gets the read lock for the given task key.
   * @param taskKey The task key to lock
   * @return Lock for the task
   */
  private Lock readLock(String taskKey) {
    return stripedLock.get(taskKey).readLock();
  }

  /**
   * Gets the write lock for the given task key.
   * @param taskKey The task key to lock
   * @return Lock for the task
   */
  private Lock writeLock(String taskKey) {
    return stripedLock.get(taskKey).writeLock();
  }

  /**
   * Checks if a task exists in the task table.
   * @param taskKey The task key to check
   * @return true if the task exists
   * @throws IOException if there is an error accessing the table
   */
  public boolean isTaskExists(String taskKey) throws IOException {
    readLock(taskKey).lock();
    try {
      return taskTable.isExist(taskKey);
    } finally {
      readLock(taskKey).unlock();
    }
  }

  /**
   * Checks if a transaction exists in the transaction table.
   * @param transactionKey The task key to check
   * @return true if the task exists
   * @throws IOException if there is an error accessing the table
   */
  public boolean isTransactionExists(String transactionKey) throws IOException {
    readLock(transactionKey).lock();
    try {
      return transactionTable.isExist(transactionKey);
    } finally {
      readLock(transactionKey).unlock();
    }
  }

  /**
   * Gets the task for a given task key.
   * @param taskKey The task key to get status for
   * @return The task status, or null if not found
   * @throws IOException if there is an error accessing the table
   */
  public JobworkerMigrationKeysTaskProto getTask(String taskKey) throws IOException {
    readLock(taskKey).lock();
    try {
      return taskTable.get(taskKey);
    } finally {
      readLock(taskKey).unlock();
    }
  }

  /**
   * Completes a migration transaction by removing the transaction entry and updating the task status.
   *
   * @param taskKey        The task key
   * @param txId           The transaction ID for constructing transaction key
   * @param failedKeyCount Number of failed keys
   * @throws IOException if there is an error updating the status
   */
  public void completeTransaction(String taskKey, long txId, int failedKeyCount)
      throws IOException {
    writeLock(taskKey).lock();
    try (BatchOperation batchOperation = metadataManager.getStore().initBatchOperation()) {
      String transactionKey = getTransactionKey(taskKey, txId);
      JobworkerMigrationKeysTxProto txProto = transactionTable.get(transactionKey);
      if (txProto == null) {
        return;
      }
      transactionTable.deleteWithBatch(batchOperation, transactionKey);
      // The number of successful keys is calculated as (total keys - failed keys).
      // This is because failed keys will be retried in subsequent commands, and only
      // the remaining failed keys are sent again. For example, if a transaction originally
      // contains 10 keys and 5 fail, the retry command will only include those 5 failed keys.
      // If all retries succeed, the final failedKeyCount will be 0, and the total number of
      // successful keys will be 10. Thus, by tracking only the final failedKeyCount, we can
      // always determine the number of successful keys as (total - failed).
      updateTaskStatusWithCountsInBatch(
          batchOperation, taskKey, txProto.getMigrationKeysCount() - failedKeyCount, failedKeyCount);
      metadataManager.getStore().commitBatchOperation(batchOperation);
      LOG.debug("Completed migration transaction {}, task {}, total keys count: {}, failed keys: {}",
          transactionKey, taskKey, txProto.getMigrationKeysCount(), failedKeyCount);
    } finally {
      writeLock(taskKey).unlock();
    }
  }

  /**
   * Updates task status with success and failure counts in a batch operation.
   * @param batchOperation The batch operation to use
   * @param taskKey The task key to update
   * @param successCount Number of successful keys
   * @param failedCount Number of failed keys
   * @throws IOException if there is an error updating the status
   */
  private void updateTaskStatusWithCountsInBatch(BatchOperation batchOperation,
      String taskKey, int successCount, int failedCount) throws IOException {
    JobworkerMigrationKeysTaskProto currentTask = taskTable.get(taskKey);
    if (currentTask != null) {
      JobworkerMigrationKeysTaskProto.Builder builder = currentTask.toBuilder();
      builder.setMigratedKeyCount(builder.getMigratedKeyCount() + successCount);
      builder.setFailedKeyCount(builder.getFailedKeyCount() + failedCount);
      builder.setLastUpdateTime(System.currentTimeMillis());
      taskTable.putWithBatch(batchOperation, taskKey, builder.build());
      LOG.debug("Updated migration status for {}: +{} succeeded, +{} failed",
          taskKey, successCount, failedCount);
    } else {
      LOG.warn("Migration task is not found for the key: {}", taskKey);
    }
  }

}
