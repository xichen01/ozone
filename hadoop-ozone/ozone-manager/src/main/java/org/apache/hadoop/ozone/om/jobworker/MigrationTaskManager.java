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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.MigrationKeyDBUpdateManager;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Manages migration tasks with striped locks to ensure thread safety.
 * This class provides synchronized access to migration task tables and status.
 * 
 * All write operations use HA-compatible writes through DBUpdateManager
 * for consistency across OM instances in HA mode.
 */
public class MigrationTaskManager {

  private final Striped<ReadWriteLock> stripedLock;
  private final Table<String, JobworkerMigrationKeysTaskProto> taskTable;
  private final Table<String, JobworkerMigrationKeysTxProto> transactionTable;
  private final MigrationKeyDBUpdateManager dbUpdateManager;
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

  public MigrationTaskManager(OMMetadataManager metadataManager, OzoneManager ozoneManager, OzoneConfiguration conf) {
    this.taskTable = metadataManager.getJobworkerMigrationKeysTaskTable();
    this.transactionTable = metadataManager.getJobworkerMigrationKeysTxTable();
    this.stripedLock = Striped.readWriteLock(LOCK_STRIPE_SIZE);
    this.dbUpdateManager = new MigrationKeyDBUpdateManager(ozoneManager);
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
   * @param transactionKey The transaction key to check
   * @return true if the transaction exists
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
   * Updates the task status with HA-compatible atomic operation.
   * @param taskKey The task key to update
   * @param newStatus The new status to set
   * @throws IOException if there is an error updating the status
   */
  public void updateTaskStatus(String taskKey, JobworkerTaskStatus newStatus) throws IOException {
    writeLock(taskKey).lock();
    try {
      dbUpdateManager.keyMigrationUpdateTaskStatus(taskKey, newStatus);
    } finally {
      writeLock(taskKey).unlock();
    }
  }

  /**
   * Completes a migration transaction by removing the transaction entry and updating the task status
   * using HA-compatible writes.
   *
   * @param taskKey        The task key
   * @param txId           The transaction ID for constructing transaction key
   * @param failedKeyCount Number of failed keys
   * @throws IOException if there is an error completing the transaction
   */
  public void completeTransaction(String taskKey, long txId, long failedKeyCount) throws IOException {
    writeLock(taskKey).lock();
    try {
      dbUpdateManager.keyMigrationCompleteTransaction(taskKey, txId, failedKeyCount);
    } finally {
      writeLock(taskKey).unlock();
    }
  }

  /**
   * Marks scanning as completed for a task using HA-compatible writes.
   * @param taskKey The task key to mark scanning as completed
   * @throws IOException if there is an error marking scanning as completed
   */
  public void markScanningCompleted(String taskKey) throws IOException {
    writeLock(taskKey).lock();
    try {
      dbUpdateManager.keyMigrationMarkScanningCompleted(taskKey);
    } finally {
      writeLock(taskKey).unlock();
    }
  }

  /**
   * Cleans up a completed task using HA-compatible writes.
   * @param taskKey The task key to clean up
   * @throws IOException if there is an error cleaning up the task
   */
  public void cleanupTask(String taskKey) throws IOException {
    writeLock(taskKey).lock();
    try {
      dbUpdateManager.keyMigrationCleanupTask(taskKey);
    } finally {
      writeLock(taskKey).unlock();
    }
  }
}
