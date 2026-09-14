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

package org.apache.hadoop.ozone.om.response.db;

import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.response.migrationKey.OMMigrationKeyDBUpdateResponse;
import org.apache.hadoop.ozone.om.response.migrationKey.OMMigrationKeyDBUpdateResponse.DBUpdateResult;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MigrationKeyDBUpdateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * Tests for {@link OMMigrationKeyDBUpdateResponse}.
 */
public class TestOMMigrationKeyDBUpdateResponse {

  @TempDir
  private Path folder;

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;
  private final Random random = new Random();

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void tearDown() {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatchCompleteMigrationTransaction() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, random.nextLong());
    JobworkerMigrationKeysTaskProto initialTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, taskKey, initialTask);
    JobworkerMigrationKeysTxProto migrationTx = createMigrationTransaction(transactionKey, taskKey, 3);
    omMetadataManager.getJobworkerMigrationKeysTxTable()
        .putWithBatch(batchOperation, transactionKey, migrationTx);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    JobworkerMigrationKeysTaskProto updatedTask = initialTask.toBuilder()
        .setMigratedKeyCount(8)  // 5 + 3
        .setFailedKeyCount(0)
        .setLastUpdateTime(System.currentTimeMillis())
        .build();
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationKeyCompleteTxResult(
        taskKey, transactionKey, updatedTask);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    Assertions.assertFalse(omMetadataManager.getJobworkerMigrationKeysTxTable()
        .isExist(transactionKey));
    
    // Verify task was updated
    JobworkerMigrationKeysTaskProto resultTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(resultTask);
    Assertions.assertEquals(8, resultTask.getMigratedKeyCount());
    Assertions.assertEquals(0, resultTask.getFailedKeyCount());
  }

  @Test
  public void testAddToDBBatchUpdateTaskStatus() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    JobworkerMigrationKeysTaskProto initialTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, taskKey, initialTask);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    JobworkerMigrationKeysTaskProto updatedTask = initialTask.toBuilder()
        .setMigrationStatus(JobworkerTaskStatus.COMPLETED)
        .setLastUpdateTime(System.currentTimeMillis())
        .build();
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationUpdateTaskStatusResult(
        taskKey, updatedTask);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    // Verify task status was updated
    JobworkerMigrationKeysTaskProto resultTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(resultTask);
    Assertions.assertEquals(JobworkerTaskStatus.COMPLETED, resultTask.getMigrationStatus());
  }

  @Test
  public void testAddToDBBatchMarkScanningCompleted() throws Exception {
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    JobworkerMigrationKeysTaskProto initialTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, taskKey, initialTask);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    JobworkerMigrationKeysTaskProto updatedTask = initialTask.toBuilder()
        .setCompleteScanning(true)
        .setLastUpdateTime(System.currentTimeMillis())
        .build();
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationMarkScanningCompletedResult(
        taskKey, updatedTask);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    
    // Initialize new batch operation
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    // Verify scanning completed flag was set
    JobworkerMigrationKeysTaskProto resultTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(resultTask);
    Assertions.assertTrue(resultTask.getCompleteScanning());
  }

  @Test
  public void testAddToDBBatchCleanupTask() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    JobworkerMigrationKeysTaskProto initialTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, taskKey, initialTask);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    Assertions.assertTrue(omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .isExist(taskKey));
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationCleanupTaskResult(taskKey);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    // Verify task was deleted
    Assertions.assertFalse(omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .isExist(taskKey));
  }

  @Test
  public void testConstructorWithNullDBUpdateResult() {
    OMResponse omResponse = createSuccessResponse();
    Assertions.assertThrows(NullPointerException.class, () -> {
      new OMMigrationKeyDBUpdateResponse(omResponse, null);
    });
  }

  @Test
  public void testAddToDBBatchCreateTask() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    JobworkerMigrationKeysTaskProto createdTask = createMigrationTask(taskKey, 0, 0, 0)
        .toBuilder()
        .setMigrationStatus(JobworkerTaskStatus.PENDING)
        .build();
    
    // Verify task doesn't exist initially
    Assertions.assertFalse(omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .isExist(taskKey));
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationCreateTaskResult(
        taskKey, createdTask);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = 
        new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    
    // Execute batch operation
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    // Verify task was created
    JobworkerMigrationKeysTaskProto resultTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(resultTask);
    Assertions.assertEquals(JobworkerTaskStatus.PENDING, resultTask.getMigrationStatus());
    Assertions.assertEquals(0, resultTask.getTotalKeyCount());
    Assertions.assertEquals(0, resultTask.getMigratedKeyCount());
    Assertions.assertEquals(0, resultTask.getFailedKeyCount());
  }

  @Test
  public void testAddToDBBatchAddTransaction() throws Exception {
    // Setup test data
    String taskKey = MigrationTaskManager.generateTaskKey("vol1", "bucket1", random.nextLong());
    String transactionKey = MigrationTaskManager.getTransactionKey(taskKey, random.nextLong());
    JobworkerMigrationKeysTaskProto initialTask = createMigrationTask(taskKey, 10, 5, 0);
    omMetadataManager.getJobworkerMigrationKeysTaskTable()
        .putWithBatch(batchOperation, taskKey, initialTask);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    JobworkerMigrationKeysTaskProto updatedTask = initialTask.toBuilder()
        .setMigratedKeyCount(8)  // 5 + 3
        .setLastUpdateTime(System.currentTimeMillis())
        .build();
    
    JobworkerMigrationKeysTxProto addedTransaction = createMigrationTransaction(
        transactionKey, taskKey, 3);
    
    // Create response
    OMResponse omResponse = createSuccessResponse();
    DBUpdateResult dbUpdateResult = DBUpdateResult.createMigrationAddTransactionResult(
        taskKey, transactionKey, updatedTask, addedTransaction);
    OMMigrationKeyDBUpdateResponse omDBUpdateResponse = 
        new OMMigrationKeyDBUpdateResponse(omResponse, dbUpdateResult);
    
    // Execute batch operation
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omDBUpdateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    
    // Verify task was updated
    JobworkerMigrationKeysTaskProto resultTask = 
        omMetadataManager.getJobworkerMigrationKeysTaskTable().get(taskKey);
    Assertions.assertNotNull(resultTask);
    Assertions.assertEquals(8, resultTask.getMigratedKeyCount());
    
    // Verify transaction was added
    JobworkerMigrationKeysTxProto resultTransaction = 
        omMetadataManager.getJobworkerMigrationKeysTxTable().get(transactionKey);
    Assertions.assertNotNull(resultTransaction);
    Assertions.assertEquals(3, resultTransaction.getMigrationKeysCount());
    Assertions.assertEquals(addedTransaction.getTxId(), resultTransaction.getTxId());
  }

  private OMResponse createSuccessResponse() {
    return OMResponse.newBuilder()
        .setMigrationKeyDBUpdateResponse(MigrationKeyDBUpdateResponse.newBuilder()
            .build())
        .setStatus(Status.OK)
        .setCmdType(Type.MigrationKeyDBUpdate)
        .setSuccess(true)
        .build();
  }

  private JobworkerMigrationKeysTaskProto createMigrationTask(String taskKey, int totalKeys, 
      int migratedKeys, int failedKeys) {
    return JobworkerMigrationKeysTaskProto.newBuilder()
        .setMigrationStatus(JobworkerTaskStatus.EXECUTING)
        .setTotalKeyCount(totalKeys)
        .setMigratedKeyCount(migratedKeys)
        .setFailedKeyCount(failedKeys)
        .setStartTime(System.currentTimeMillis())
        .setLastUpdateTime(System.currentTimeMillis())
        .setCompleteScanning(false)
        .setRuleId(RandomStringUtils.randomAlphabetic(32))
        .setEcReplicationConfig(new ECReplicationConfig("rs-6-3-1024k").toProto())
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
        txId = 1; // fallback
      }
    }
    
    JobworkerMigrationKeysTxProto.Builder builder = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(txId)
        .setVolume("test-volume")
        .setBucket("test-bucket")
        .setEcReplicationConfig(new ECReplicationConfig("rs-6-3-1024k").toProto())
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
