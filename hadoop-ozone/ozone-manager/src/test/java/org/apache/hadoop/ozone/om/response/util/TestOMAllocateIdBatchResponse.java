/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.response.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.util.OMSequenceIdGenerator;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateIdBatchResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests for {@link OMAllocateIdBatchResponse}.
 */
public class TestOMAllocateIdBatchResponse {

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @BeforeEach
  public void setup(@TempDir File tempDir) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, tempDir.getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, tempDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void cleanup() {
    if (batchOperation != null) {
      batchOperation.close();
    }
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testAddToDBBatchWithMultipleSequences() throws Exception {
    // Test with TEST1 sequence
    String sequenceIdName1 = "tes1";
    long newLastId1 = 500L;
    
    OMResponse omResponse1 = createSuccessfulOMResponse(1L, 500L);
    OMAllocateIdBatchResponse response1 = 
        new OMAllocateIdBatchResponse(omResponse1, sequenceIdName1, newLastId1);
    response1.addToDBBatch(omMetadataManager, batchOperation);
    
    // Test with TEST2 sequence
    String sequenceIdName2 = "tes2";
    long newLastId2 = 300L;
    
    OMResponse omResponse2 = createSuccessfulOMResponse(1L, 300L);
    OMAllocateIdBatchResponse response2 = 
        new OMAllocateIdBatchResponse(omResponse2, sequenceIdName2, newLastId2);
    
    response2.addToDBBatch(omMetadataManager, batchOperation);
    // Commit the batch operation
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // Verify both sequences were persisted
    Long persistedLastId1 = getSequenceIdTableValue(omMetadataManager, sequenceIdName1);
    Long persistedLastId2 = getSequenceIdTableValue(omMetadataManager, sequenceIdName2);
    
    assertNotNull(persistedLastId1);
    assertNotNull(persistedLastId2);
    assertEquals(newLastId1, persistedLastId1.longValue());
    assertEquals(newLastId2, persistedLastId2.longValue());
  }

  @Test
  public void testAddToDBBatchWithSequentialUpdates() throws Exception {
    String sequenceIdName = "test1";
    // First update
    long newLastId1 = 100L;
    OMResponse omResponse1 = createSuccessfulOMResponse(1L, 100L);
    OMAllocateIdBatchResponse response1 = 
        new OMAllocateIdBatchResponse(omResponse1, sequenceIdName, newLastId1);
    
    response1.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // Verify first update
    assertEquals(newLastId1, getSequenceIdTableValue(omMetadataManager, sequenceIdName));
    
    // Second update
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    long newLastId2 = 200L;
    OMResponse omResponse2 = createSuccessfulOMResponse(101L, 200L);
    OMAllocateIdBatchResponse response2 = 
        new OMAllocateIdBatchResponse(omResponse2, sequenceIdName, newLastId2);

    response2.addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    // Verify second update
    assertEquals(newLastId2, getSequenceIdTableValue(omMetadataManager, sequenceIdName));
  }

  private Long getSequenceIdTableValue(OMMetadataManager metadataManager, String sequenceIdName) throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, Long>> iterator =
             metadataManager.getSequenceIdTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, Long> value = iterator.next();
        if (value.getKey().equals(sequenceIdName)) {
          return value.getValue();
        }
      }
    }
    return null;
  }

  private OMResponse createSuccessfulOMResponse(long startId, long endId) {
    AllocateIdBatchResponse allocateIdBatchResponse = 
        AllocateIdBatchResponse.newBuilder()
            .setStartId(startId)
            .setEndId(endId)
            .build();
    return OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateIdBatch)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setAllocateIdBatchResponse(allocateIdBatchResponse)
        .build();
  }
}
