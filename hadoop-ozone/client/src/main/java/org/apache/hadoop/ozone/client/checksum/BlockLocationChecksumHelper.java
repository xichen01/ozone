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

package org.apache.hadoop.ozone.client.checksum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;

/**
 * Computes a key-level composite checksum from the checksums stored in blocks.
 */
public final class BlockLocationChecksumHelper {
  private BlockLocationChecksumHelper() {
  }

  /** Holds the computed key checksum and its source checksum type. */
  public static final class KeyChecksumInfo {
    private final FileChecksum fileChecksum;
    private final ContainerProtos.ChecksumType checksumType;

    KeyChecksumInfo(FileChecksum fileChecksum,
        ContainerProtos.ChecksumType checksumType) {
      this.fileChecksum = fileChecksum;
      this.checksumType = checksumType;
    }

    public FileChecksum getFileChecksum() {
      return fileChecksum;
    }

    public ContainerProtos.ChecksumType getChecksumType() {
      return checksumType;
    }
  }

  public static KeyChecksumInfo computeFileChecksum(
      XceiverClientFactory xceiverClientFactory,
      List<OzoneKeyLocation> blockLocations,
      ReplicationConfig replicationConfig) throws IOException {
    if (blockLocations.isEmpty()) {
      return null;
    }
    ContainerProtos.ChecksumType checksumType = null;
    int bytesPerChecksum = -1;
    long[] blockLengths = new long[blockLocations.size()];
    try (DataOutputBuffer blockChecksumBuffer = new DataOutputBuffer()) {
      for (int i = 0; i < blockLocations.size(); i++) {
        OzoneKeyLocation location = blockLocations.get(i);
        blockLengths[i] = location.getLength();
        List<ContainerProtos.ChunkInfo> chunks = fetchChunkInfos(
            xceiverClientFactory, location, replicationConfig);
        if (chunks.isEmpty()) {
          throw new IOException("No chunk info found for block " + location.getBlockID());
        }
        ContainerProtos.ChecksumData checksumData = chunks.get(0).getChecksumData();
        if (checksumType == null) {
          checksumType = checksumData.getType();
          bytesPerChecksum = checksumData.getBytesPerChecksum();
        }
        if (checksumData.getType() != checksumType) {
          throw new IOException("Checksum type differs between key blocks");
        }
        AbstractBlockChecksumComputer computer;
        if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
          computer = new ECBlockChecksumComputer(chunks,
              (ECReplicationConfig) replicationConfig, location.getLength());
        } else {
          computer = new ReplicatedBlockChecksumComputer(chunks, location.getLength());
        }
        computer.compute(OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC);
        blockChecksumBuffer.write(computer.getOutByteBuffer().array());
      }
      FileChecksum checksum = BaseFileChecksumHelper.composeBlockCRCsToFileChecksum(
          checksumType, bytesPerChecksum, blockChecksumBuffer.getData(),
          blockLocations.size(), blockLengths, blockLengths[0]);
      return new KeyChecksumInfo(checksum, checksumType);
    }
  }

  private static List<ContainerProtos.ChunkInfo> fetchChunkInfos(
      XceiverClientFactory xceiverClientFactory, OzoneKeyLocation location,
      ReplicationConfig replicationConfig) throws IOException {
    Pipeline pipeline = location.getPipeline();
    if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
      pipeline = createEcChecksumPipeline(pipeline);
    } else {
      pipeline = pipeline.copyForRead();
    }
    XceiverClientSpi client = null;
    try {
      client = xceiverClientFactory.acquireClientForReadData(pipeline);
      ContainerProtos.GetBlockResponseProto response =
          ContainerProtocolCalls.getBlock(client, location.getBlockID(),
              location.getToken(), pipeline);
      return response.getBlockData().getChunksList();
    } finally {
      if (client != null) {
        xceiverClientFactory.releaseClientForReadData(client, false);
      }
    }
  }

  private static Pipeline createEcChecksumPipeline(Pipeline source) {
    ECReplicationConfig ecConfig = (ECReplicationConfig) source.getReplicationConfig();
    List<DatanodeDetails> nodes = new ArrayList<>();
    java.util.Map<DatanodeDetails, Integer> indexes = new java.util.HashMap<>();
    for (DatanodeDetails dn : source.getNodes()) {
      int index = source.getReplicaIndex(dn);
      if (index == 1 || index > ecConfig.getData()) {
        nodes.add(dn);
        indexes.put(dn, index);
      }
    }
    PipelineID id = PipelineID.randomId();
    return Pipeline.newBuilder()
        .setId(id)
        .setReplicationConfig(org.apache.hadoop.hdds.client.StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setState(source.getPipelineState())
        .setNodes(nodes)
        .setReplicaIndexes(indexes)
        .build();
  }
}
