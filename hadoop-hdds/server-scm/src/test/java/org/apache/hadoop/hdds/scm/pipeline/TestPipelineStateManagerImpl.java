/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for PipelineStateManagerImpl.
 */
public class TestPipelineStateManagerImpl {

  private PipelineStateManager stateManager;
  @TempDir
  private File testDir;
  private DBStore dbStore;

  @BeforeEach
  public void init() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());

    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(true);
    NodeManager nodeManager = new MockNodeManager(true, 10);

    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  private Pipeline createDummyPipeline(int numNodes) {
    return createDummyPipeline(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, numNodes);
  }

  private Pipeline createDummyPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, int numNodes) {
    return createDummyPipeline(type, factor, numNodes, StorageTier.getDefaultTier());
  }

  private Pipeline createDummyPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, int numNodes, StorageTier storageTier) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < numNodes; i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    return Pipeline.newBuilder()
        .setReplicationConfig(
            ReplicationConfig.fromProtoTypeAndFactor(type, factor))
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.ALLOCATED)
        .setId(PipelineID.randomId())
        .setSupportedStorageTier(storageTier)
        .build();
  }

  @Test
  public void testAddAndGetPipeline() throws IOException, TimeoutException {
    Exception e = assertThrows(SCMException.class,
        () -> stateManager.addPipeline(createDummyPipeline(0)
            .getProtobufMessage(ClientVersion.CURRENT_VERSION)));
    // replication factor and number of nodes in the pipeline do not match
    assertThat(e.getMessage()).contains("do not match");

    // add a pipeline
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);

    try {
      stateManager.addPipeline(pipelineProto);

      // Cannot add a pipeline twice
      e = assertThrows(SCMException.class,
          () -> stateManager.addPipeline(pipelineProto));
      assertThat(e.getMessage()).contains("Duplicate pipeline ID");

      // verify pipeline returned is same
      assertEquals(pipeline.getId(),
          stateManager.getPipeline(pipeline.getId()).getId());
    } finally {
      // clean up
      finalizePipeline(pipelineProto);
      removePipeline(pipelineProto);
    }
  }

  @Test
  public void testGetPipelines() throws IOException, TimeoutException {
    // In start there should be no pipelines
    assertTrue(stateManager.getPipelines().isEmpty());

    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    HddsProtos.Pipeline pipeline = createDummyPipeline(1).getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipeline);
    pipelines.add(pipeline);
    pipeline = createDummyPipeline(1).getProtobufMessage(
        ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipeline);
    pipelines.add(pipeline);

    Set<Pipeline> pipelines1 = new HashSet<>(stateManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.ONE)));
    assertEquals(pipelines1.size(), pipelines.size());

    pipelines1 = new HashSet<>(stateManager.getPipelines());
    assertEquals(pipelines1.size(), pipelines.size());

    // clean up
    for (HddsProtos.Pipeline pipeline1 : pipelines) {
      finalizePipeline(pipeline1);
      removePipeline(pipeline1);
    }
  }

  @Test
  public void testGetPipelinesByTypeAndFactorAndStorageTier()
      throws IOException, TimeoutException {

    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (StorageTier storageTier : StorageTier.values()) {
          if (storageTier == StorageTier.EMPTY) {
            continue;
          }
          for (int i = 0; i < 5; i++) {
            // 5 pipelines in allocated state for each type and factor and storageTier
            HddsProtos.Pipeline pipeline =
                createDummyPipeline(type, factor, factor.getNumber(), storageTier)
                    .getProtobufMessage(ClientVersion.CURRENT_VERSION);
            stateManager.addPipeline(pipeline);
            pipelines.add(pipeline);

            // 5 pipelines in open state for each type and factor
            pipeline = createDummyPipeline(type, factor, factor.getNumber(), storageTier)
                .getProtobufMessage(ClientVersion.CURRENT_VERSION);
            stateManager.addPipeline(pipeline);
            pipelines.add(pipeline);

            // 5 pipelines in closed state for each type and factor
            pipeline = createDummyPipeline(type, factor, factor.getNumber(), storageTier)
                .getProtobufMessage(ClientVersion.CURRENT_VERSION);
            stateManager.addPipeline(pipeline);
            pipelines.add(pipeline);
          }
        }
      }
    }

    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (StorageTier storageTier : StorageTier.values()) {
          if (storageTier == StorageTier.EMPTY || factor == ReplicationFactor.ZERO) {
            continue;
          }
          // verify pipelines received
          List<Pipeline> pipelines1 =
              stateManager.getPipelines(ReplicationConfig.fromTypeAndFactor(
                      org.apache.hadoop.hdds.client.ReplicationType.fromProto(type),
                      org.apache.hadoop.hdds.client.ReplicationFactor.fromProto(factor)),
                  storageTier);
          assertEquals(15, pipelines1.size());
          pipelines1.forEach(p -> assertEquals(type, p.getType()));
          pipelines1.forEach(p -> assertEquals(storageTier, p.getSupportedStorageTier()));
        }
        List<Pipeline> pipelines1 =
            stateManager.getPipelines(
                ReplicationConfig.fromProtoTypeAndFactor(type, factor));
        assertEquals(15 * (StorageTier.values().length - 1), pipelines1.size());
        pipelines1.stream().forEach(p -> {
          assertEquals(type, p.getType());
        });
      }
    }

    //clean up
    for (HddsProtos.Pipeline pipeline : pipelines) {
      finalizePipeline(pipeline);
      removePipeline(pipeline);
    }
  }

  @Test
  public void testGetPipelinesByTypeFactorAndState()
      throws IOException, TimeoutException {
    Set<HddsProtos.Pipeline> pipelines = new HashSet<>();
    for (HddsProtos.ReplicationType type : new ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (int i = 0; i < 5; i++) {
          // 5 pipelines in allocated state for each type and factor
          HddsProtos.Pipeline pipeline =
              createDummyPipeline(type, factor, factor.getNumber())
                  .getProtobufMessage(ClientVersion.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in open state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersion.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          openPipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in dormant state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersion.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          openPipeline(pipeline);
          deactivatePipeline(pipeline);
          pipelines.add(pipeline);

          // 5 pipelines in closed state for each type and factor
          pipeline = createDummyPipeline(type, factor, factor.getNumber())
              .getProtobufMessage(ClientVersion.CURRENT_VERSION);
          stateManager.addPipeline(pipeline);
          finalizePipeline(pipeline);
          pipelines.add(pipeline);
        }
      }
    }

    for (HddsProtos.ReplicationType type : new HddsProtos.ReplicationType[] {
        ReplicationType.RATIS, ReplicationType.STAND_ALONE}) {
      for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
          .values()) {
        for (Pipeline.PipelineState state : Pipeline.PipelineState.values()) {
          // verify pipelines received
          List<Pipeline> pipelines1 =
              stateManager.getPipelines(
                  ReplicationConfig.fromProtoTypeAndFactor(type, factor),
                  state);
          assertEquals(5, pipelines1.size());
          pipelines1.forEach(p -> {
            assertEquals(type, p.getType());
            assertEquals(state, p.getPipelineState());
          });
        }
      }
    }

    //clean up
    for (HddsProtos.Pipeline pipeline : pipelines) {
      finalizePipeline(pipeline);
      removePipeline(pipeline);
    }
  }

  @Test
  public void testAddAndGetContainer() throws IOException, TimeoutException {
    long containerID = 0;
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    pipeline = stateManager.getPipeline(pipeline.getId());
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));

    // move pipeline to open state
    openPipeline(pipelineProto);
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));

    //verify the number of containers returned
    Set<ContainerID> containerIDs =
        stateManager.getContainers(pipeline.getId());
    assertEquals(containerIDs.size(), containerID);

    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
    Pipeline finalPipeline = pipeline;
    ContainerID cid = ContainerID.valueOf(++containerID);
    IOException e =
        assertThrows(IOException.class,
            () -> stateManager.addContainerToPipeline(finalPipeline.getId(), cid));
    // Can not add a container to removed pipeline
    assertThat(e.getMessage()).contains("not found");
  }

  @Test
  public void testRemovePipeline() throws IOException, TimeoutException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // close the pipeline
    openPipeline(pipelineProto);
    stateManager
        .addContainerToPipeline(pipeline.getId(), ContainerID.valueOf(1));

    IOException e = assertThrows(IOException.class, () -> removePipeline(pipelineProto));
    // can not remove a pipeline which already has containers
    assertThat(e.getMessage()).contains("not yet closed");

    // close the pipeline
    finalizePipeline(pipelineProto);
    // remove containers and then remove the pipeline
    removePipeline(pipelineProto);
  }

  @Test
  public void testRemoveContainer() throws IOException, TimeoutException {
    long containerID = 1;
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    // create an open pipeline in stateMap
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);

    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    assertEquals(1,
        stateManager.getContainers(pipeline.getId()).size());
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    assertEquals(0,
        stateManager.getContainers(pipeline.getId()).size());

    // add two containers in the pipeline
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    stateManager.addContainerToPipeline(pipeline.getId(),
        ContainerID.valueOf(++containerID));
    assertEquals(2,
        stateManager.getContainers(pipeline.getId()).size());

    // move pipeline to closing state
    finalizePipeline(pipelineProto);

    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(containerID));
    stateManager.removeContainerFromPipeline(pipeline.getId(),
        ContainerID.valueOf(--containerID));
    assertEquals(0,
        stateManager.getContainers(pipeline.getId()).size());

    // clean up
    removePipeline(pipelineProto);
  }

  @Test
  public void testFinalizePipeline() throws IOException, TimeoutException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // finalize on ALLOCATED pipeline
    finalizePipeline(pipelineProto);
    assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);

    pipeline = createDummyPipeline(1);
    pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);
    // finalize on OPEN pipeline
    finalizePipeline(pipelineProto);
    assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);

    pipeline = createDummyPipeline(1);
    pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    openPipeline(pipelineProto);
    finalizePipeline(pipelineProto);
    // finalize should work on already closed pipeline
    finalizePipeline(pipelineProto);
    assertEquals(Pipeline.PipelineState.CLOSED,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    removePipeline(pipelineProto);
  }

  @Test
  public void testOpenPipeline() throws IOException, TimeoutException {
    Pipeline pipeline = createDummyPipeline(1);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    // open on ALLOCATED pipeline
    openPipeline(pipelineProto);
    assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());

    openPipeline(pipelineProto);
    // open should work on already open pipeline
    assertEquals(Pipeline.PipelineState.OPEN,
        stateManager.getPipeline(pipeline.getId()).getPipelineState());
    // clean up
    finalizePipeline(pipelineProto);
    removePipeline(pipelineProto);
  }

  @Test
  public void testQueryPipeline() throws IOException, TimeoutException {
    for (StorageTier storageTier : StorageTier.values()) {
      if (storageTier == StorageTier.EMPTY) {
        continue;
      }
      Pipeline pipeline = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, 3, storageTier);
      // pipeline in allocated state should not be reported
      HddsProtos.Pipeline pipelineProto = pipeline
          .getProtobufMessage(ClientVersion.CURRENT_VERSION);
      stateManager.addPipeline(pipelineProto);
      assertEquals(0, stateManager
          .getPipelines(RatisReplicationConfig
                  .getInstance(ReplicationFactor.THREE),
              Pipeline.PipelineState.OPEN)
          .size());

      // pipeline in open state should be reported
      openPipeline(pipelineProto);
      assertEquals(1, stateManager
          .getPipelines(RatisReplicationConfig
                  .getInstance(ReplicationFactor.THREE),
              Pipeline.PipelineState.OPEN)
          .size());

      Pipeline pipeline2 = createDummyPipeline(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, 3, storageTier);
      pipeline2 = pipeline2.toBuilder()
          .setState(Pipeline.PipelineState.OPEN)
          .build();
      HddsProtos.Pipeline pipelineProto2 = pipeline2
          .getProtobufMessage(ClientVersion.CURRENT_VERSION);
      // pipeline in open state should be reported
      stateManager.addPipeline(pipelineProto2);
      assertEquals(2, stateManager
          .getPipelines(RatisReplicationConfig
                  .getInstance(ReplicationFactor.THREE),
              Pipeline.PipelineState.OPEN)
          .size());

      // pipeline in closed state should not be reported
      finalizePipeline(pipelineProto2);
      assertEquals(1, stateManager
          .getPipelines(RatisReplicationConfig
                  .getInstance(ReplicationFactor.THREE),
              Pipeline.PipelineState.OPEN)
          .size());

      // clean up
      finalizePipeline(pipelineProto);
      removePipeline(pipelineProto);
      finalizePipeline(pipelineProto2);
      removePipeline(pipelineProto2);
    }
  }

  private HddsProtos.Pipeline createAndAddDummyPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, int numNodes, StorageTier storageTier)
      throws IOException, TimeoutException {
    Pipeline pipeline = createDummyPipeline(type,
        factor, numNodes, storageTier);
    HddsProtos.Pipeline pipelineProto = pipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(pipelineProto);
    return pipelineProto;
  }

  private void assertQueryPipeline(int expectSize, PipelineState state,
      HddsProtos.ReplicationFactor factor, StorageTier storageTier) {
    assertEquals(expectSize, stateManager.getPipelines(
            RatisReplicationConfig.getInstance(factor), state, storageTier).size());
  }

  private void assertQueryPipeline(int expectSize, PipelineState state,
      HddsProtos.ReplicationFactor factor) {
    assertEquals(expectSize, stateManager.getPipelines(
            RatisReplicationConfig.getInstance(factor), state).size());
  }

  @Test
  public void testQueryPipelineWithStorageTier() throws IOException, TimeoutException {
    // pipeline in open state should be reported
    HddsProtos.Pipeline diskTierPipeline = createAndAddDummyPipeline(
        ReplicationType.RATIS, ReplicationFactor.THREE, 3, StorageTier.DISK);
    openPipeline(diskTierPipeline);
    // diskTierPipeline
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.DISK);
    assertQueryPipeline(0, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.SSD);

    HddsProtos.Pipeline ssdTierPipeline = createAndAddDummyPipeline(
        ReplicationType.RATIS, ReplicationFactor.THREE, 3, StorageTier.SSD);
    openPipeline(ssdTierPipeline);
    // diskTierPipeline + ssdTierPipeline
    assertQueryPipeline(2, PipelineState.OPEN, ReplicationFactor.THREE);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.DISK);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.SSD);

    HddsProtos.Pipeline archiveTierPipeline = createAndAddDummyPipeline(
        ReplicationType.RATIS, ReplicationFactor.THREE, 3, StorageTier.ARCHIVE);
    openPipeline(archiveTierPipeline);
    // diskTierPipeline + ssdTierPipeline + archiveTierPipeline
    assertQueryPipeline(3, PipelineState.OPEN, ReplicationFactor.THREE);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.DISK);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.SSD);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.ARCHIVE);

    // pipeline in closed state should not be reported
    finalizePipeline(diskTierPipeline);
    // ssdTierPipeline + archiveTierPipeline
    assertQueryPipeline(2, PipelineState.OPEN, ReplicationFactor.THREE);
    assertQueryPipeline(0, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.DISK);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.SSD);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.ARCHIVE);

    // Legacy pipelines without StorageTier are treated as the default tier.
    Pipeline legacyPipeline = Pipeline.newBuilder()
        .setReplicationConfig(
            ReplicationConfig.fromProtoTypeAndFactor(ReplicationType.RATIS,
                ReplicationFactor.THREE))
        .setNodes(Arrays.asList(
            MockDatanodeDetails.randomDatanodeDetails(),
            MockDatanodeDetails.randomDatanodeDetails(),
            MockDatanodeDetails.randomDatanodeDetails()))
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .build();
    HddsProtos.Pipeline legacyPipelineProto = legacyPipeline
        .getProtobufMessage(ClientVersion.CURRENT_VERSION);
    stateManager.addPipeline(legacyPipelineProto);
    assertQueryPipeline(3, PipelineState.OPEN, ReplicationFactor.THREE);
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE,
        StorageTier.getDefaultTier());
    assertQueryPipeline(1, PipelineState.OPEN, ReplicationFactor.THREE, StorageTier.SSD);

    // clean up
    finalizePipeline(diskTierPipeline);
    removePipeline(diskTierPipeline);
    finalizePipeline(ssdTierPipeline);
    removePipeline(ssdTierPipeline);
    finalizePipeline(archiveTierPipeline);
    removePipeline(archiveTierPipeline);
    finalizePipeline(legacyPipelineProto);
    removePipeline(legacyPipelineProto);
  }

  private void removePipeline(HddsProtos.Pipeline pipeline)
      throws IOException, TimeoutException {
    stateManager.removePipeline(pipeline.getId());
  }

  private void openPipeline(HddsProtos.Pipeline pipeline)
      throws IOException, TimeoutException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_OPEN);
  }

  private void finalizePipeline(HddsProtos.Pipeline pipeline)
      throws IOException, TimeoutException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_CLOSED);
  }

  private void deactivatePipeline(HddsProtos.Pipeline pipeline)
      throws IOException, TimeoutException {
    stateManager.updatePipelineState(pipeline.getId(),
        HddsProtos.PipelineState.PIPELINE_DORMANT);
  }
}
