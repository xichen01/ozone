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
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode;
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

  @BeforeEach
  public void setUp() throws Exception {
    OzoneConfiguration conf = createNewTestPath();

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
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
