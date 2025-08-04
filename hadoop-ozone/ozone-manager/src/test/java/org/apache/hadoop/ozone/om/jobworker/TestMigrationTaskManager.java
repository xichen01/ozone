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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StoragePolicyProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link MigrationTaskManager} with real OM in HA mode.
 */
public class TestMigrationTaskManager {

  @TempDir
  private File tempDir;

  private OzoneConfiguration conf;
  private OmTestManagers testManagers;
  private MigrationTaskManager migrationTaskManager;
  private OMMetadataManager metadataManager;
  private OzoneManager ozoneManager;

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());

    testManagers = new OmTestManagers(conf);
    ozoneManager = testManagers.getOzoneManager();
    metadataManager = testManagers.getMetadataManager();
    migrationTaskManager = new MigrationTaskManager(metadataManager, ozoneManager, conf);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (testManagers != null) {
      ozoneManager.stop();
    }
  }

  @Test
  public void testMigrationTaskOp() throws Exception {
    String taskKey = "migration-task-lifecycle";
    long txId = 12345L;
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);

    // 1. Test basic task operations - initially task should not exist
    Assertions.assertFalse(migrationTaskManager.isTaskExists(taskKey));
    Assertions.assertNull(migrationTaskManager.getTask(taskKey));

    // Create a task
    JobworkerMigrationKeysTaskProto task = createMigrationTask(50, 10, 0);
    metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);

    // Verify task exists and has correct initial state
    Assertions.assertTrue(migrationTaskManager.isTaskExists(taskKey));
    JobworkerMigrationKeysTaskProto retrievedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertNotNull(retrievedTask);
    Assertions.assertEquals(JobworkerTaskStatus.PENDING, retrievedTask.getMigrationStatus());
    Assertions.assertEquals(50, retrievedTask.getTotalKeyCount());

    // 2. Test task status update
    migrationTaskManager.updateTaskStatus(taskKey, JobworkerTaskStatus.EXECUTING);
    JobworkerMigrationKeysTaskProto updatedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertEquals(JobworkerTaskStatus.EXECUTING, updatedTask.getMigrationStatus());

    // 3. Test transaction operations
    Assertions.assertFalse(migrationTaskManager.isTransactionExists(transactionKey));
    
    // Create and complete transaction
    JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, 5);
    metadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, transaction);
    Assertions.assertTrue(migrationTaskManager.isTransactionExists(transactionKey));
    
    migrationTaskManager.completeTransaction(taskKey, txId, 1); // 1 failed key
    Assertions.assertFalse(migrationTaskManager.isTransactionExists(transactionKey));
    
    // Verify task counts updated: 10 + (5 - 1) = 14 migrated, 0 + 1 = 1 failed
    updatedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertEquals(14, updatedTask.getMigratedKeyCount());
    Assertions.assertEquals(1, updatedTask.getFailedKeyCount());

    // 4. Test mark scanning completed
    Assertions.assertFalse(updatedTask.getCompleteScanning());
    migrationTaskManager.markScanningCompleted(taskKey);
    updatedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertTrue(updatedTask.getCompleteScanning());

    // 5. Test cleanup task
    migrationTaskManager.cleanupTask(taskKey);
    Assertions.assertFalse(migrationTaskManager.isTaskExists(taskKey));
    Assertions.assertNull(migrationTaskManager.getTask(taskKey));
  }

  @Test
  public void testConcurrentTransactionCompletion() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(30);
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    long totalKeyCountPerTask = 50;
    long txCountPerTask = 10;
    long failedKeyPerTx = 1;

    try {
      // Create 3 tasks with initial counts
      List<String> taskKeys = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        String taskKey = "concurrent-task-" + i;
        taskKeys.add(taskKey);
        
        // Create task with some initial migrated keys
        JobworkerMigrationKeysTaskProto task = createMigrationTask(totalKeyCountPerTask, 0, 0);
        metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
      }
      
      // Create transactions for each task (10 transactions per task)
      List<TransactionInfo> allTransactions = new ArrayList<>();
      int index = 0;
      for (String taskKey : taskKeys) {
        for (int txIndex = 0; txIndex < txCountPerTask; txIndex++) {
          long txId = index * 1000L + txIndex;
          String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
          JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, 5);
          metadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, transaction);
          
          allTransactions.add(new TransactionInfo(taskKey, txId, transactionKey, failedKeyPerTx));
        }
        index++;
      }
      
      // Verify all transactions exist
      for (TransactionInfo txInfo : allTransactions) {
        Assertions.assertTrue(migrationTaskManager.isTransactionExists(txInfo.transactionKey));
      }
      
      // Execute all transactions in parallel
      for (TransactionInfo txInfo : allTransactions) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            migrationTaskManager.completeTransaction(txInfo.taskKey, txInfo.txId, txInfo.failedKeys);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, executor);
        futures.add(future);
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
      
      // Verify all transactions are removed
      for (TransactionInfo txInfo : allTransactions) {
        Assertions.assertFalse(migrationTaskManager.isTransactionExists(txInfo.transactionKey));
      }

      // Verify task counts are correct
      for (int i = 0; i < taskKeys.size(); i++) {
        String taskKey = taskKeys.get(i);
        JobworkerMigrationKeysTaskProto finalTask = migrationTaskManager.getTask(taskKey);
        Assertions.assertNotNull(finalTask);
        long expectedFailed = txCountPerTask * failedKeyPerTx;
        long expectedMigrated = totalKeyCountPerTask - expectedFailed;
        Assertions.assertEquals(expectedMigrated, finalTask.getMigratedKeyCount(),
            "Task " + taskKey + " migrated count mismatch");
        Assertions.assertEquals(expectedFailed, finalTask.getFailedKeyCount(),
            "Task " + taskKey + " failed count mismatch");
      }
      
    } finally {
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  // Helper class to store transaction information
  private static class TransactionInfo {
    private final String taskKey;
    private final long txId;
    private final String transactionKey;
    private final long failedKeys;
    
    TransactionInfo(String taskKey, long txId, String transactionKey, long failedKeys) {
      this.taskKey = taskKey;
      this.txId = txId;
      this.transactionKey = transactionKey;
      this.failedKeys = failedKeys;
    }
  }

  private JobworkerMigrationKeysTaskProto createMigrationTask(long totalKeys, long migratedKeys, long failedKeys) {
    return JobworkerMigrationKeysTaskProto.newBuilder()
        .setMigrationStatus(JobworkerTaskStatus.PENDING)
        .setTotalKeyCount(totalKeys)
        .setMigratedKeyCount(migratedKeys)
        .setFailedKeyCount(failedKeys)
        .setStartTime(System.currentTimeMillis())
        .setLastUpdateTime(System.currentTimeMillis())
        .setCompleteScanning(false)
        .build();
  }

  private JobworkerMigrationKeysTxProto createMigrationTransaction(String transactionKey, 
      String taskKey, int keyCount) {
    JobworkerMigrationKeysTxProto.Builder builder = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(System.currentTimeMillis())
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setStoragePolicy(StoragePolicyProto.HOT)
        .setTaskKey(taskKey);
    
    for (int i = 0; i < keyCount; i++) {
      builder.addMigrationKeys(
          MigrationKeyProto.newBuilder()
              .setKey("key-" + i)
              .setUpdateID(i)
              .build());
    }
    
    return builder.build();
  }
}
