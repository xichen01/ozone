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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.util;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link OMSequenceIdGenerator}.
 */
public class TestOMSequenceIdGenerator {

  private static final String TEST1 = "test1";
  private static final String TEST2 = "test2";
  private OmTestManagers omTestManagers;
  private OzoneManager ozoneManager;
  private OMSequenceIdGenerator sequenceIdGenerator;
  private OzoneConfiguration conf;

  public void setup(@TempDir Path tempDir, OzoneConfiguration ozoneConfiguration) throws Exception {
    conf = ozoneConfiguration;
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    sequenceIdGenerator = new OMSequenceIdGenerator(conf, ozoneManager);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.stop();
    }
  }

  @Test
  public void testMultipleSequenceTypes(@TempDir Path tempDir) throws Exception {
    setup(tempDir, new OzoneConfiguration());
    long test1Id = sequenceIdGenerator.getNextId(TEST1);
    long test2Id = sequenceIdGenerator.getNextId(TEST2);

    // All should be valid IDs (positive)
    assertTrue(test1Id > 0);
    assertTrue(test2Id > 0);

    // Each sequence should start from 1 (or configured start value)
    // Second call should be incremented
    assertEquals(test1Id + 1, sequenceIdGenerator.getNextId(TEST1));
    assertEquals(test2Id + 1, sequenceIdGenerator.getNextId(TEST2));
  }

  @Test
  public void testBatchAllocationWithCustomBatchSize(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_SEQUENCE_ID_BATCH_SIZE, String.valueOf(1));
    setup(tempDir, ozoneConfiguration);
    long id1 = sequenceIdGenerator.getNextId(TEST1);
    long id2 = sequenceIdGenerator.getNextId(TEST1);
    long id3 = sequenceIdGenerator.getNextId(TEST1);
    long id4 = sequenceIdGenerator.getNextId(TEST1);
    long id5 = sequenceIdGenerator.getNextId(TEST1);

    // All should be sequential
    assertEquals(id1 + 1, id2);
    assertEquals(id2 + 1, id3);
    assertEquals(id3 + 1, id4);
    assertEquals(id4 + 1, id5);
  }

  @Test
  public void testBatchInvalidation(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    setup(tempDir, new OzoneConfiguration());
    long id1 = sequenceIdGenerator.getNextId(TEST1);
    sequenceIdGenerator.invalidateAllBatches();
    long id2 = sequenceIdGenerator.getNextId(TEST1);
    assertEquals((id2 - id1), ozoneConfiguration.getInt(
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE,
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE_DEFAULT));
  }

  @Test
  @Timeout(30)
  public void testConcurrentAccess(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    setup(tempDir, ozoneConfiguration);
    long initialId = sequenceIdGenerator.getNextId(TEST1);
    final int numThreads = 20;
    final int idsPerThread = ozoneConfiguration.getInt(
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE,
        OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE_DEFAULT) * 2;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    Set<Long> allIds = new ConcurrentSkipListSet<>();
    CompletableFuture<Void>[] futures = new CompletableFuture[numThreads];

    for (int i = 0; i < numThreads; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
        Set<Long> threadIds = new HashSet<>();
        try {
          for (int j = 0; j < idsPerThread; j++) {
            long id = sequenceIdGenerator.getNextId(TEST1);
            threadIds.add(id);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        assertEquals(idsPerThread, threadIds.size());
        // Check no duplicate IDs across threads
        for (Long id : threadIds) {
          assertTrue(allIds.add(id), "Duplicate ID found: " + id);
        }
      }, executor);
    }

    // Wait for all threads to complete
    CompletableFuture.allOf(futures).get();
    executor.shutdown();
    assertEquals(numThreads * idsPerThread, allIds.size());
    assertEquals(initialId + (long) numThreads * (long) idsPerThread + 1,
        sequenceIdGenerator.getNextId(TEST1));
  }

  @Test
  public void testSequenceIDGenWhenCurrentOMIsNotALeader(@TempDir Path tempDir) throws Exception {
    int batchSize = 5;
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.setInt(OZONE_OM_SEQUENCE_ID_BATCH_SIZE, batchSize);
    setup(tempDir, ozoneConfiguration);

    // Create a custom OMSequenceIdGenerator that simulates non-leader behavior
    OMSequenceIdGenerator customGenerator = new OMSequenceIdGenerator(ozoneConfiguration, ozoneManager) {
      private boolean firstBatchAllocated = false;

      @Override
      protected OMResponse submitRequest(OMRequest omRequest) throws IOException, ServiceException {
        // Allow the first batch allocation to succeed
        if (!firstBatchAllocated) {
          firstBatchAllocated = true;
          return super.submitRequest(omRequest);
        }
        throw new OMNotLeaderException("OM is not a leader");
      }
    };

    // Get the first ID, which should work since it's within the initial batch
    assertEquals(1L, customGenerator.getNextId(TEST1));

    // Simulation: currently this OM is not a leader node,
    // So this OM can only allocate IDs within the current batch
    // ([1, batchSize]), does not allow the allocation of IDs for the next batch
    // ([batchSize + 1, batchSize * 2])
    for (int i = 0; i < batchSize * 3; i++) {
      try {
        long nextID = customGenerator.getNextId(TEST1);
        if (nextID > batchSize) {
          fail("Should not allocate a sequence ID: " + nextID +
              " that exceeds the current Batch: " + batchSize);
        }
      } catch (OMException e) {
        // Expected exception when trying to allocate beyond current batch
        // and OM is not a leader
        assertInstanceOf(OMNotLeaderException.class, e.getCause());
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void testOverflowProtection(@TempDir Path tempDir) throws Exception {
    // Test that overflow is properly detected and handled
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    setup(tempDir, ozoneConfiguration);
    long currentLastId = Long.MAX_VALUE - ozoneConfiguration.getInt(OZONE_OM_SEQUENCE_ID_BATCH_SIZE,
        OZONE_OM_SEQUENCE_ID_BATCH_SIZE_DEFAULT) + 1;
    
    ozoneManager.getMetadataManager().getSequenceIdTable()
        .addCacheEntry(new CacheKey<>("test-sequence"),
            CacheValue.get(1L, currentLastId));
    try {
      sequenceIdGenerator.getNextId("test-sequence");
      fail("Expected OMException due to overflow");
    } catch (OMException e) {
      assertTrue(e.getMessage().contains("overflow"));
    }
  }
}
