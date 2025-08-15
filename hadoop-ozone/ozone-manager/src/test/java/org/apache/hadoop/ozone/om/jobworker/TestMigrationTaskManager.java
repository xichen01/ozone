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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StoragePolicyProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  private final Random random = new Random();

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
    migrationTaskManager.markScanningCompletedIfPresent(taskKey);
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
          
          allTransactions.add(new TransactionInfo(taskKey, txId, transactionKey, (int)failedKeyPerTx));
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

  @Test
  public void testCreateTaskIfAbsent() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    Assertions.assertFalse(migrationTaskManager.isTaskExists(taskKey));
    Assertions.assertTrue(migrationTaskManager.createTaskIfAbsent(taskKey, "RuleId1", OzoneStoragePolicy.COLD));
    
    // Verify task was created
    Assertions.assertTrue(migrationTaskManager.isTaskExists(taskKey));
    JobworkerMigrationKeysTaskProto createdTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertNotNull(createdTask);
    Assertions.assertEquals(JobworkerTaskStatus.PENDING, createdTask.getMigrationStatus());
    Assertions.assertEquals("RuleId1", createdTask.getRuleId());
    Assertions.assertEquals(OzoneStoragePolicy.COLD,
        OzoneStoragePolicy.fromProto(createdTask.getStoragePolicy()));
    Assertions.assertTrue(createdTask.getStartTime() > 0);
    Assertions.assertTrue(createdTask.getLastUpdateTime() > 0);

    // Call again - should not create duplicate
    long originalStartTime = createdTask.getStartTime();
    Assertions.assertFalse(migrationTaskManager.createTaskIfAbsent(taskKey, "RuleId1", OzoneStoragePolicy.COLD));
    JobworkerMigrationKeysTaskProto unchangedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertEquals(originalStartTime, unchangedTask.getStartTime());
  }

  @Test
  public void testAddNewTransaction() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = 98765L;
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    
    JobworkerMigrationKeysTaskProto task = createMigrationTask(10, 5, 0);
    metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, 3);
    migrationTaskManager.addNewTransaction(taskKey, transaction);
    
    // Verify transaction was added
    Assertions.assertTrue(migrationTaskManager.isTransactionExists(transactionKey));

    // Verify task migrated count was updated
    JobworkerMigrationKeysTaskProto updatedTask = migrationTaskManager.getTask(taskKey);
    Assertions.assertEquals(5, updatedTask.getMigratedKeyCount());
    Assertions.assertEquals(13, updatedTask.getTotalKeyCount()); // 10 + 3
  }

  @Test
  public void testAddNewTransactionTaskNotExists() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    long txId = 98765L;
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, 3);
    
    // Should throw exception for non-existent task
    Assertions.assertThrows(IOException.class, () -> {
      migrationTaskManager.addNewTransaction(taskKey, transaction);
    });
  }

  @Test
  public void testListTransactionsForTask() throws Exception {
    // Create transactions for task1 (5 transactions)
    String task1Key = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    List<String> expectedTask1Tx = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      long txId = 1000L + i;
      String transactionKey = MigrationTaskManager.getTransactionKey(task1Key, txId);
      expectedTask1Tx.add(transactionKey);
      JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, task1Key, i + 1);
      metadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, transaction);
    }
    
    // Create transactions for task2 (3 transactions)
    String task2Key = MigrationTaskManager.generateTaskKey("vol2", "bucket2", random.nextLong());
    List<String> expectedTask2Tx = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      long txId = 2000L + i;
      String transactionKey = MigrationTaskManager.getTransactionKey(task2Key, txId);
      expectedTask2Tx.add(transactionKey);
      JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, task2Key, i + 1);
      metadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, transaction);
    }

    // Test pagination with different limits for both tasks
    testPaginationWithDifferentLimits(task1Key, expectedTask1Tx);
    testPaginationWithDifferentLimits(task2Key, expectedTask2Tx);
  }

  private void testPaginationWithDifferentLimits(String taskKey, List<String> expectedKeys) throws IOException {
    for (int limit = 1; limit <= expectedKeys.size(); limit++) {
      List<MigrationTaskManager.TransactionEntry> allTransactions = new ArrayList<>();
      String startKey = null;
      int loopCount = 0;
      int maxLoops = expectedKeys.size() + 1; // Prevent infinite loops
      
      // Fetch all transactions using the specified limit
      while (loopCount < maxLoops) {
        List<MigrationTaskManager.TransactionEntry> pageResult = migrationTaskManager.listTransactionsForTask(
            taskKey, startKey, limit);
        allTransactions.addAll(pageResult);
        
        if (pageResult.size() < limit) {
          break; // Reached the end
        }
        
        startKey = pageResult.get(pageResult.size() - 1).getTransactionKey();
        loopCount++;
      }
      
      Assertions.assertEquals(expectedKeys.size(), allTransactions.size());
      // Verify no duplicates and all expected keys are present
      Set<String> actualKeys = new HashSet<>();
      for (MigrationTaskManager.TransactionEntry entry : allTransactions) {
        Assertions.assertTrue(expectedKeys.contains(entry.getTransactionKey()));
        Assertions.assertTrue(entry.getTransactionKey().startsWith(taskKey + "/"));
        Assertions.assertNotNull(entry.getTransaction());
        actualKeys.add(entry.getTransactionKey());
      }
      Assertions.assertEquals(expectedKeys.size(), actualKeys.size());
    }
  }

  @Test
  public void testListTransactionsForTaskWithCacheAndDeletes() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    
    // Create transactions in DB
    List<String> dbTransactionKeys = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      long txId = 1000L + i;
      String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
      dbTransactionKeys.add(transactionKey);
      
      JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, i + 1);
      metadataManager.getJobworkerMigrationKeysTxTable().put(transactionKey, transaction);
    }
    
    // Add some transactions to cache
    List<String> cacheTransactionKeys = new ArrayList<>();
    for (int i = 3; i < 5; i++) {
      long txId = 1000L + i;
      String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
      cacheTransactionKeys.add(transactionKey);
      
      JobworkerMigrationKeysTxProto transaction = createMigrationTransaction(transactionKey, taskKey, i + 1);
      metadataManager.getJobworkerMigrationKeysTxTable().addCacheEntry(
          new CacheKey<>(transactionKey), 
          CacheValue.get(i, transaction));
    }
    
    // Mark one DB transaction as deleted in cache
    String deletedKey = dbTransactionKeys.get(1);
    metadataManager.getJobworkerMigrationKeysTxTable().addCacheEntry(
        new CacheKey<>(deletedKey), 
        CacheValue.get(6L));
    
    // List transactions
    List<MigrationTaskManager.TransactionEntry> transactions = 
        migrationTaskManager.listTransactionsForTask(taskKey, null, Integer.MAX_VALUE);
    
    // Should have 4 transactions (3 from DB - 1 deleted + 2 from cache)
    Assertions.assertEquals(4, transactions.size());
    
    // Verify deleted transaction is not included
    boolean foundDeleted = false;
    for (MigrationTaskManager.TransactionEntry entry : transactions) {
      if (entry.getTransactionKey().equals(deletedKey)) {
        foundDeleted = true;
        break;
      }
    }
    Assertions.assertFalse(foundDeleted);
    
    // Verify cache transactions are included
    for (String cacheKey : cacheTransactionKeys) {
      boolean foundCacheEntry = false;
      for (MigrationTaskManager.TransactionEntry entry : transactions) {
        if (entry.getTransactionKey().equals(cacheKey)) {
          foundCacheEntry = true;
          break;
        }
      }
      Assertions.assertTrue(foundCacheEntry, "Cache transaction not found: " + cacheKey);
    }
  }

  @Test
  public void testListTask() throws Exception {
    String volume = "test-volume";
    String bucket = "test-bucket";
    
    // Create multiple tasks for the volume/bucket
    List<String> expectedTaskKeys = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(volume, bucket, i);
      expectedTaskKeys.add(taskKey);
      
      JobworkerMigrationKeysTaskProto task = createMigrationTask(10 + i, 5 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    }
    
    // Create some tasks for different volume/bucket to ensure filtering
    String otherVolume = "other-volume";
    String otherBucket = "other-bucket";
    for (int i = 0; i < 2; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(otherVolume, otherBucket, i);
      JobworkerMigrationKeysTaskProto task = createMigrationTask(20 + i, 10 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    }
    
    // List tasks for our volume/bucket
    List<MigrationTaskManager.TaskEntry> tasks = 
        migrationTaskManager.listTask(volume, bucket);
    
    // Verify correct tasks were returned
    Assertions.assertEquals(4, tasks.size());
    for (MigrationTaskManager.TaskEntry entry : tasks) {
      Assertions.assertTrue(expectedTaskKeys.contains(entry.getTaskKey()));
      Assertions.assertTrue(entry.getTaskKey().startsWith(volume + "/" + bucket + "/"));
      Assertions.assertNotNull(entry.getTask());
    }
    
    // Verify tasks are sorted by key
    for (int i = 1; i < tasks.size(); i++) {
      Assertions.assertTrue(
          tasks.get(i - 1).getTaskKey().compareTo(
              tasks.get(i).getTaskKey()) < 0);
    }
  }

  @Test
  public void testGetAllTasks() throws Exception {
    // Create tasks across different volumes/buckets
    String volume1 = "vol1";
    String bucket1 = "bucket1";
    String volume2 = "vol2";
    String bucket2 = "bucket2";
    
    List<String> allTaskKeys = new ArrayList<>();
    
    // Tasks for vol1/bucket1
    for (int i = 0; i < 2; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(volume1, bucket1, i);
      allTaskKeys.add(taskKey);
      JobworkerMigrationKeysTaskProto task = createMigrationTask(10 + i, 5 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    }
    
    // Tasks for vol2/bucket2
    for (int i = 0; i < 3; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(volume2, bucket2, i);
      allTaskKeys.add(taskKey);
      JobworkerMigrationKeysTaskProto task = createMigrationTask(20 + i, 10 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    }
    
    // Get all tasks
    List<MigrationTaskManager.TaskEntry> allTasks = migrationTaskManager.getAllTasks();
    
    // Verify all tasks were returned
    Assertions.assertEquals(5, allTasks.size());
    for (MigrationTaskManager.TaskEntry entry : allTasks) {
      Assertions.assertTrue(allTaskKeys.contains(entry.getTaskKey()));
      Assertions.assertNotNull(entry.getTask());
    }
  }

  @Test
  public void testListTaskWithCacheAndDeletes() throws Exception {
    String volume = "cache-test-vol";
    String bucket = "cache-test-bucket";
    
    // Create tasks in DB
    List<String> dbTaskKeys = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(volume, bucket, i);
      dbTaskKeys.add(taskKey);
      JobworkerMigrationKeysTaskProto task = createMigrationTask(10 + i, 5 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().put(taskKey, task);
    }
    
    // Add some tasks to cache
    List<String> cacheTaskKeys = new ArrayList<>();
    for (int i = 3; i < 5; i++) {
      String taskKey = MigrationTaskManager.generateTaskKey(volume, bucket, i);
      cacheTaskKeys.add(taskKey);
      JobworkerMigrationKeysTaskProto task = createMigrationTask(30 + i, 15 + i, i);
      metadataManager.getJobworkerMigrationKeysTaskTable().addCacheEntry(
          new CacheKey<>(taskKey), 
          CacheValue.get(1L, task));
    }
    
    // Mark one DB task as deleted in cache
    String deletedKey = dbTaskKeys.get(1);
    metadataManager.getJobworkerMigrationKeysTaskTable().addCacheEntry(
        new CacheKey<>(deletedKey), 
        CacheValue.get(2L));
    
    // List tasks
    List<MigrationTaskManager.TaskEntry> tasks = migrationTaskManager.listTask(volume, bucket);
    
    // Should have 4 tasks (3 from DB - 1 deleted + 2 from cache)
    Assertions.assertEquals(4, tasks.size());
    
    // Verify deleted task is not included
    boolean foundDeleted = false;
    for (MigrationTaskManager.TaskEntry entry : tasks) {
      if (entry.getTaskKey().equals(deletedKey)) {
        foundDeleted = true;
        break;
      }
    }
    Assertions.assertFalse(foundDeleted);
    
    // Verify cache tasks are included
    for (String cacheKey : cacheTaskKeys) {
      boolean foundCacheEntry = false;
      for (MigrationTaskManager.TaskEntry entry : tasks) {
        if (entry.getTaskKey().equals(cacheKey)) {
          foundCacheEntry = true;
          break;
        }
      }
      Assertions.assertTrue(foundCacheEntry, "Cache task not found: " + cacheKey);
    }
  }

  // Helper class to store transaction information
  private static class TransactionInfo {
    private final String taskKey;
    private final long txId;
    private final String transactionKey;
    private final int failedKeys;
    
    TransactionInfo(String taskKey, long txId, String transactionKey, int failedKeys) {
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
        .setRuleId(RandomStringUtils.randomAlphanumeric(32))
        .setStoragePolicy(StoragePolicyProto.WARM)
        .build();
  }

  private JobworkerMigrationKeysTxProto createMigrationTransaction(String transactionKey, 
      String taskKey, int keyCount) {
    // Extract transaction ID from the transaction key for proto
    long txId = 1;
    if (transactionKey.contains("/")) {
      String[] parts = transactionKey.split("/");
      try {
        txId = Long.parseLong(parts[parts.length - 1]);
      } catch (NumberFormatException e) {
        txId = System.currentTimeMillis(); // fallback to timestamp
      }
    }
    
    JobworkerMigrationKeysTxProto.Builder builder = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(txId)
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
