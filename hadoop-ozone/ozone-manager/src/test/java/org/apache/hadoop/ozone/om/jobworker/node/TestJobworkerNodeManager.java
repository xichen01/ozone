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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.SendHeartbeatRequest;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPortType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.util.RemoteAddressInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test {@link JobworkerNodeManager}
 */
@ExtendWith(MockitoExtension.class)
public class TestJobworkerNodeManager {
  private JobworkerNodeManager nodeManager;
  private OzoneManager ozoneManager;
  @TempDir
  private Path folder;
  private String omServiceId1;

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = createNewTestPath();

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    omServiceId1 = ozoneManager.getOMServiceId();
    nodeManager = ozoneManager.getJobworkerNodemanager();
  }

  @Test
  public void testRegisterJobworker() throws Exception {
    // Prepare
    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.getHostAddress()).thenReturn("1.2.3.4");
    when(mockInetAddress.getHostName()).thenReturn("test-hostname");
    Context ctx = Context.current().withValue(RemoteAddressInterceptor.REMOTE_ADDR, mockInetAddress);
    UUID jobworkerUuid = UUID.randomUUID();
    JobworkerDetails jobworkerDetails = JobworkerDetails.newBuilder()
        .setUuid(jobworkerUuid)
        .addPort(JobworkerPortType.HTTP, 100)
        .build();
    Context originalContext = ctx.attach();
    try {
      RegisterJobworkerResponse response = nodeManager.registerJobworker(
          jobworkerDetails);

      // Verify response
      assertNotNull(response);
      assertEquals(ReturnCode.SUCCESS, response.getReturnCode());
      assertEquals(ozoneManager.getOMServiceId(), response.getOmServiceId());
      assertEquals(jobworkerUuid.getMostSignificantBits(), response.getJobworkerUUID().getMostSigBits());
      assertEquals(jobworkerUuid.getLeastSignificantBits(), response.getJobworkerUUID().getLeastSigBits());
      assertEquals(ozoneManager.getOmStorage().getClusterID(), response.getClusterID());
      assertEquals(ozoneManager.getNodeDetails().getServiceId(), response.getOmServiceId());
      assertEquals("test-hostname", response.getHostname());
      assertEquals("1.2.3.4", response.getIpAddress());
      assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));
    } finally {
      ctx.detach(originalContext);
    }
  }

  @Test
  public void testJobworkerNodeReport() throws Exception {
    // Prepare
    JobworkerDetails jobworkerDetails = registerJobworker();
    UUID jobworkerUuid = jobworkerDetails.getUuid();
    final long capacity = 2000;
    final long remaining = 1900;
    JobworkerStorageReportProto storageReport =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(jobworkerUuid.toString())
            .setStorageLocation(folder.toString())
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setFailed(false)
            .build();
    JobworkerNodeReportProto nodeReport = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .build();

    nodeManager.processNodeReport(jobworkerDetails, nodeReport);

    // Verify storage reports were updated
    JobworkerInfo nodeInfo = nodeManager.getNodeStateManager().getNodeInfo(jobworkerUuid);
    List<JobworkerStorageReportProto> storageSummary = nodeInfo.getStorageReports();

    assertEquals(1, storageSummary.size());
    assertEquals(capacity, storageSummary.get(0).getCapacity());
    assertEquals(remaining, storageSummary.get(0).getRemaining());
    assertEquals(jobworkerUuid.toString(), storageSummary.get(0).getStorageUuid());

    // Test with multiple storage reports
    JobworkerStorageReportProto storageReport2 =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(UUID.randomUUID().toString())
            .setStorageLocation(folder.toString() + "/second")
            .setCapacity(capacity)
            .setRemaining(remaining)
            .setFailed(false)
            .build();
    JobworkerNodeReportProto nodeReport2 = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .addStorageReport(storageReport2)
        .build();

    // Process the updated node report
    nodeManager.processNodeReport(jobworkerDetails, nodeReport2);

    // Verify the updated storage reports
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(2, storageSummary.size());
    assertEquals(capacity * 2, storageSummary.stream().mapToLong(JobworkerStorageReportProto::getCapacity).sum());
    assertEquals(remaining * 2, storageSummary.stream().mapToLong(JobworkerStorageReportProto::getRemaining).sum());

    // Test with one failed volume
    JobworkerStorageReportProto failedReport =
        JobworkerStorageReportProto.newBuilder()
            .setStorageUuid(UUID.randomUUID().toString())
            .setStorageLocation(folder.toString() + "/failed")
            .setCapacity(0)
            .setRemaining(0)
            .setFailed(true)
            .build();
    JobworkerNodeReportProto nodeReport3 = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .addStorageReport(storageReport2)
        .addStorageReport(failedReport)
        .build();

    // Process the report with a failed volume
    nodeManager.processNodeReport(jobworkerDetails, nodeReport3);

    // Verify the stats including the failed volume
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(3, storageSummary.size());
    assertEquals(1, storageSummary.stream().filter(JobworkerStorageReportProto::getFailed).count());
    assertEquals(2, nodeInfo.getHealthyVolumeCount());

    // Test processing a node report from an unregistered node
    JobworkerDetails unregisteredNode = MockJobworkerDetails.randomJobworkerDetails();
    JobworkerNodeReportProto unregReport = JobworkerNodeReportProto.newBuilder()
        .addStorageReport(storageReport)
        .build();

    nodeManager.processNodeReport(unregisteredNode, unregReport);
    storageSummary = nodeInfo.getStorageReports();
    assertEquals(3, storageSummary.size());
  }

  @Test
  public void testJobworkerHeartbeat() throws Exception {
    JobworkerDetails jobworkerDetails = registerJobworker();
    UUID jobworkerUuid = jobworkerDetails.getUuid();
    SendHeartbeatRequest heartbeatRequest = createHeartbeatRequest(jobworkerDetails.getProtoBufMessage());

    long heartbeatTime1 = nodeManager.getNodeStateManager()
        .getNodeInfo(jobworkerUuid).getLastHeartbeatTime();
    assertTrue(nodeManager.isJobworkerNodeRegistered(jobworkerUuid));

    ozoneManager.getJobworkerServerProtocol().sendHeartbeat(heartbeatRequest);
    long heartbeatTime2 = nodeManager.getNodeStateManager()
        .getNodeInfo(jobworkerUuid).getLastHeartbeatTime();
    assertTrue(heartbeatTime2 > heartbeatTime1);
  }

  private JobworkerDetails registerJobworker() throws IOException {
    JobworkerDetails jobworkerDetails = MockJobworkerDetails.randomLocalJobworkerDetails();
    RegisterJobworkerResponse registerResponse = nodeManager.registerJobworker(
        jobworkerDetails);
    assertEquals(ReturnCode.SUCCESS, registerResponse.getReturnCode());
    return jobworkerDetails;
  }

  private SendHeartbeatRequest createHeartbeatRequest(JobworkerDetailsProto jobworkerDetailProto) {
    return SendHeartbeatRequest.newBuilder()
        .setJobworkerDetails(jobworkerDetailProto)
        .setOmServiceId(omServiceId1)
        .build();
  }

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      Assertions.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.close();
    }
  }
}
