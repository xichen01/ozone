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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ServiceException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.util.OMSequenceIdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests OMSequenceIdGenerator in HA mode during transferLeadership.
 */
@Timeout(900)
public class TestOMSequenceIdGeneratorHA extends TestOzoneManagerHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOMSequenceIdGeneratorHA.class);

  private static final String SEQUENCE_NAME1 = "testSequence1";
  private static final String SEQUENCE_NAME2 = "testSequence2";
  private static final int BATCH_SIZE = 5;
  private static final int TRANSFER_COUNT = 5;
  private static final int OM_HA_SERVICE_INDEX = 0; // Only need to test one of OM ha SERVICE

  @Test
  public void testSequenceIdGeneratorDuringTransferLeadership() throws Exception {
    getConf().setInt(OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE, BATCH_SIZE);
    ExecutorService executorService = Executors.newCachedThreadPool();
    
    try {
      AtomicBoolean idGenerationRunning = new AtomicBoolean(true);
      Map<Long, String> allocatedId = new ConcurrentHashMap();
      List<CompletableFuture<Void>> idGenerationFutures = new ArrayList<>();

      // Start ID generation thread
      for (OzoneManager ozoneManager : getCluster().getOzoneManagersList(OM_HA_SERVICE_INDEX)) {
        idGenerationFutures.add(CompletableFuture.runAsync(() -> {
          OMSequenceIdGenerator sequenceIdGenerator = new OMSequenceIdGenerator(getConf(), ozoneManager);
          long prevId = -1;
          try {
            while (idGenerationRunning.get()) {
              while (!ozoneManager.isLeaderReady() && idGenerationRunning.get()) {
                Thread.sleep(50);
              }
              while (idGenerationRunning.get()) {
                try {
                  long id = sequenceIdGenerator.getNextId(SEQUENCE_NAME1);
                  assertNull(allocatedId.put(id, ozoneManager.getOMNodeId()),
                      String.format("id %s, %s be contained in %s", id, allocatedId.get(id), allocatedId));
                  assertThat(prevId).isLessThan(id);
                  prevId = id;
                  Thread.sleep(ThreadLocalRandom.current().nextInt(10) + 1);
                } catch (ServiceException e) {
                  if (e.getCause() instanceof OMNotLeaderException) {
                    break;
                  }
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException("Error in ID generation thread", e);
          }
        }, executorService));
      }

      // Start leadership transfer thread
      CompletableFuture<Void> transferFuture = CompletableFuture.runAsync(() -> {
        try {
          for (int i = 0; i < TRANSFER_COUNT; i++) {
            OzoneManager currentLeader = getCluster().getOMLeader(OM_HA_SERVICE_INDEX);
            OzoneManager targetFollower = null;
            
            for (OzoneManager om : getCluster().getOzoneManagersList(OM_HA_SERVICE_INDEX)) {
              if (om != currentLeader) {
                targetFollower = om;
                break;
              }
            }
            Assertions.assertNotNull(targetFollower, "No follower OM found");
            currentLeader.transferLeadership(targetFollower.getOMNodeId());

            Thread.sleep(ThreadLocalRandom.current().nextInt(2000) + 1);
            OzoneManager newLeader = getCluster().getOMLeader(OM_HA_SERVICE_INDEX, true);
            Assertions.assertNotNull(newLeader, "New leader should be ready");
            assertTrue(newLeader.isLeaderReady(), "New leader should be ready");
            LOG.info("Leadership transferred from {} to {} (transfer {}/" + TRANSFER_COUNT + ")",
                currentLeader.getOMNodeId(), newLeader.getOMNodeId(), i + 1);
          }
        } catch (Exception e) {
          throw new RuntimeException("Error in leadership transfer thread", e);
        }
      }, executorService);
      
      // Wait for the leadership transfer thread to complete
      transferFuture.get(300, TimeUnit.SECONDS);
      idGenerationRunning.set(false);

      // Wait for the ID generation thread to complete
      CompletableFuture.allOf(idGenerationFutures.toArray(new CompletableFuture[0]))
          .get(60, TimeUnit.SECONDS);
    } finally {
      executorService.shutdown();
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    }
  }

  @Test
  public void testSequenceIdGeneratorDuringLeaderOMShutdown() throws Exception {
    getConf().setInt(OMConfigKeys.OZONE_OM_SEQUENCE_ID_BATCH_SIZE, BATCH_SIZE);
    ExecutorService executorService = Executors.newCachedThreadPool();
    
    try {
      AtomicBoolean idGenerationRunning = new AtomicBoolean(true);
      Map<Long, String> allocatedId = new ConcurrentHashMap();
      List<CompletableFuture<Void>> idGenerationFutures = new ArrayList<>();

      // Start ID generation thread
      for (OzoneManager ozoneManager : getCluster().getOzoneManagersList(OM_HA_SERVICE_INDEX)) {
        OMSequenceIdGenerator sequenceIdGenerator = new OMSequenceIdGenerator(getConf(), ozoneManager);
        idGenerationFutures.add(CompletableFuture.runAsync(() -> {
          long prevId = -1;
          try {
            while (idGenerationRunning.get()) {
              while (!ozoneManager.isLeaderReady() && idGenerationRunning.get()) {
                Thread.sleep(50);
              }
              while (idGenerationRunning.get() && ozoneManager.isRunning()) {
                try {
                  long id = sequenceIdGenerator.getNextId(SEQUENCE_NAME2);
                  assertNull(allocatedId.put(id, ozoneManager.getOMNodeId()),
                      String.format("id %s, %s be contained in %s", id, allocatedId.get(id), allocatedId));
                  assertThat(prevId).isLessThan(id);
                  prevId = id;
                  Thread.sleep(ThreadLocalRandom.current().nextInt(10) + 1);
                } catch (ServiceException e) {
                  if (e.getCause() instanceof OMNotLeaderException) {
                    break;
                  }
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException("Error in ID generation thread", e);
          }
        }, executorService));
      }

      // Start stop leader OM thread
      CompletableFuture<Void> transferFuture = CompletableFuture.runAsync(() -> {
        try {
          for (int i = 0; i < TRANSFER_COUNT; i++) {
            OzoneManager currentLeader = getCluster().getOMLeader(OM_HA_SERVICE_INDEX);
            getCluster().shutdownOzoneManager(currentLeader);
            OzoneManager newLeader = getCluster().getOMLeader(OM_HA_SERVICE_INDEX, true);
            Assertions.assertNotNull(newLeader, "New leader should be ready");
            assertTrue(newLeader.isLeaderReady(), "New leader should be ready");
            LOG.info("Leadership transferred from {} to {} (transfer {}/" + TRANSFER_COUNT + ")",
                currentLeader.getOMNodeId(), newLeader.getOMNodeId(), i + 1);
            getCluster().restartOzoneManager(currentLeader, true);
          }
        } catch (Exception e) {
          throw new RuntimeException("Error in leadership transfer thread", e);
        }
      }, executorService);
      
      // Wait for the leadership transfer thread to complete
      transferFuture.get(300, TimeUnit.SECONDS);
      idGenerationRunning.set(false);

      // Wait for the ID generation thread to complete
      CompletableFuture.allOf(idGenerationFutures.toArray(new CompletableFuture[0]))
          .get(60, TimeUnit.SECONDS);
    } finally {
      executorService.shutdown();
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    }
  }
}
