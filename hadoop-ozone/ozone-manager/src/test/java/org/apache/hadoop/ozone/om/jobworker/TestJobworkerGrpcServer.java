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

package org.apache.hadoop.ozone.om.jobworker;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.ozone.jobworker.client.JobworkerClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


/**
 * Tests for Job Worker RPC server framework.
 */
public class TestJobworkerGrpcServer {

  private JobworkerGrpcServer server;
  private JobworkerClient client;
  private JobworkerProtocolServerImpl jobworkerProtocolServer;

  @BeforeEach
  public void setUp() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    JobworkerNodeManager jobworkerNodemanager = mock(JobworkerNodeManager.class);
    when(jobworkerNodemanager.getVersion(any())).thenReturn(
        GetOMVersionResponse.newBuilder().setSoftwareVersion(0).build());
    when(ozoneManager.getJobworkerNodemanager()).thenReturn(jobworkerNodemanager);
    jobworkerProtocolServer =
        spy(new JobworkerProtocolServerImpl(ozoneManager, mock(JobworkerNodeManager.class)));
    server = new JobworkerGrpcServer(conf, jobworkerProtocolServer, null);
    server.start();
    client = new JobworkerClient("localhost", conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testGetVersion() throws IOException {
    GetOMVersionResponse response = client.getOMVersion();
    assertNotNull(response);
    verify(jobworkerProtocolServer, times(1)).getOMVersion(any());
    verify(jobworkerProtocolServer, times(0)).registerJobworker(any());
    verify(jobworkerProtocolServer, times(0)).sendHeartbeat(any());
  }


  @Test
  public void testServerGracefulShutdown() throws IOException, InterruptedException {
    GetOMVersionResponse response = client.getOMVersion();
    assertNotNull(response);
    server.stop();
    Exception exception = assertThrows(StatusRuntimeException.class, () -> {
      client.getOMVersion();
    });
    assertTrue(exception.getMessage().contains("UNAVAILABLE") ||
        exception.getMessage().contains("Connection refused"));
  }

  @Test
  public void testErrorHandling() throws IOException {
    GetOMVersionResponse initialResponse = client.getOMVersion();
    assertNotNull(initialResponse);
    // Set up mock to throw exception for getOMVersion
    doThrow(new IOException("Simulated server error"))
        .when(jobworkerProtocolServer).getOMVersion(any());
    Exception exception = assertThrows(StatusRuntimeException.class, () -> {
      client.getOMVersion();
    });

    assertTrue(exception.getMessage().contains("INTERNAL") ||
        exception.getMessage().contains("Simulated server error"));
    Mockito.reset(jobworkerProtocolServer);

    try {
      GetOMVersionResponse heartbeatResponse = client.getOMVersion();
      assertNotNull(heartbeatResponse);
      verify(jobworkerProtocolServer, times(1)).getOMVersion(any());
    } catch (Exception e) {
      fail("Server should be operational after the error, but got an exception: " + e.getMessage());
    }
  }
}
