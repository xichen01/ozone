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

package org.apache.hadoop.ozone.jobworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.upgrade.JobworkerVersion;
import org.apache.hadoop.ozone.JobworkerService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.ozone.jobworker.volume.VolatileJobworkerVolume;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerInfo;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeStateManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test JobWorker functionality with Ozone HA.
 */
@Timeout(300)
public class TestJobworkerWithOzoneHA {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestJobworkerWithOzoneHA.class);

  private static final int NUM_OF_OMS_PER_SERVICE = 3;
  private static final int NUM_OF_JOBWORKERS = 2;
  private static final int NUM_OF_VOLUMES_PER_JOBWORKER = 2;

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneConfiguration conf;
  private static List<String> omServiceIds;

  @TempDir
  private File testDir;

  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    JobworkerClientConfiguration jobworkerClientConfiguration =
        conf.getObject(JobworkerClientConfiguration.class);
    jobworkerClientConfiguration.setHeartbeatInterval(Duration.of(5, ChronoUnit.SECONDS));
    conf.setFromObject(jobworkerClientConfiguration);

    // Configure two OM services with 3 OMs each
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    omServiceIds = Arrays.asList("om-service-1", "om-service-2");

    // Build and start the cluster
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterID)
        .setScmId(scmID)
        .setOMServiceIds(omServiceIds)
        .setNumOfOMsPerService(NUM_OF_OMS_PER_SERVICE)
        .setNumOfJobworkers(NUM_OF_JOBWORKERS)
        .setNumOfVolumesPerJobworker(NUM_OF_VOLUMES_PER_JOBWORKER)
        .build();

    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testJobworkerRegister() {
    List<JobworkerService> jobworkers = cluster.getJobworkers();
    assertJobworkerRunning(jobworkers);

    for (OzoneManager ozoneManager : cluster.getAllOzoneManagers()) {
      JobworkerNodeStateManager stateManager = ozoneManager.getJobworkerNodemanager().getNodeStateManager();
      for (JobworkerInfo jobworkerInfo : stateManager.getAllJobworkerInfos()) {
        assertTrue(jobworkerInfo.getNodeStatus().isHealthy());
      }
      assertEquals(NUM_OF_JOBWORKERS, stateManager.getTotalNodeCount());
    }
  }

  @Test
  public void testJobworkerHeartbeat() throws Exception {
    List<JobworkerService> jobworkers = cluster.getJobworkers();
    assertJobworkerRunning(jobworkers);

    Map<String, Long> heartBeatTimes = new HashMap<>();
    for (OzoneManager ozoneManager : cluster.getAllOzoneManagers()) {
      String omServiceId = ozoneManager.getOMServiceId();
      String omNodeId = ozoneManager.getOMNodeId();
      JobworkerNodeStateManager nodeManager = ozoneManager.getJobworkerNodemanager().getNodeStateManager();
      for (JobworkerInfo jobworkerInfo : nodeManager.getAllJobworkerInfos()) {
        heartBeatTimes.put(String.join("-", omServiceId, omNodeId, jobworkerInfo.getUuidString()),
            jobworkerInfo.getLastHeartbeatTime());
      }
    }
    assertEquals(NUM_OF_OMS_PER_SERVICE * omServiceIds.size() * NUM_OF_JOBWORKERS, heartBeatTimes.size());

    for (OzoneManager ozoneManager : cluster.getOzoneManagersList()) {
      JobworkerNodeStateManager nodeManager = ozoneManager.getJobworkerNodemanager().getNodeStateManager();
      GenericTestUtils.waitFor(() -> {
            for (JobworkerInfo jobworkerInfo : nodeManager.getAllJobworkerInfos()) {
              String omServiceId = ozoneManager.getOMServiceId();
              String omNodeId = ozoneManager.getOMNodeId();
              if (jobworkerInfo.getLastHeartbeatTime() <= heartBeatTimes.get(
                  String.join("-", omServiceId, omNodeId, jobworkerInfo.getUuidString()))) {
                LOG.info("omServiceId {}, omNodeId {} jobworker UUID {} heartbeat do not update",
                    omServiceId, omNodeId, jobworkerInfo.getUuidString());
                return false;
              }
            }
            return true;
          }, 2000, 3000000
      );
    }
  }

  @Test
  public void testJobworkerInfo() {
    List<JobworkerService> jobworkers = cluster.getJobworkers();
    assertEquals(NUM_OF_JOBWORKERS, jobworkers.size());
    assertJobworkerRunning(jobworkers);

    Map<UUID, JobworkerDetails> originalJobworkerDetails = new HashMap<>();
    for (JobworkerService jobworker : jobworkers) {
      originalJobworkerDetails.put(jobworker.getJobworkerDetails().getUuid(),
          jobworker.getJobworkerDetails());
    }

    for (OzoneManager ozoneManager : cluster.getAllOzoneManagers()) {
      JobworkerNodeStateManager nodeManager = ozoneManager.getJobworkerNodemanager().getNodeStateManager();
      for (JobworkerInfo jobworkerInfo : nodeManager.getAllJobworkerInfos()) {
        JobworkerDetails original = originalJobworkerDetails.get(jobworkerInfo.getUuid());
        assertNotNull(original);
        assertEquals(original.getHostName(), jobworkerInfo.getHostName());
        assertEquals(original.getIpAddress(), jobworkerInfo.getIpAddress());
        assertEquals(original.getNetworkName(), jobworkerInfo.getNetworkName());
        assertEquals(original.getNetworkLocation(), jobworkerInfo.getNetworkLocation());
        assertEquals(original.getVersion(), jobworkerInfo.getVersion());
        assertEquals(original.getSetupTime(), jobworkerInfo.getSetupTime());
        assertEquals(original.getBuildDate(), jobworkerInfo.getBuildDate());
        assertEquals(JobworkerVersion.CURRENT, jobworkerInfo.getJobworkerVersion());
      }
    }
  }

  @Test
  public void testJobworkerVolumeOperations() throws Exception {
    List<JobworkerService> jobworkers = cluster.getJobworkers();

    for (int i = 0; i < jobworkers.size(); i++) {
      JobworkerService jobworker = jobworkers.get(i);
      JobworkerVolumeSet volumeSet = jobworker.getJobworkerStateMachine().getVolumeSet();
      assertNotNull(volumeSet);
      List<VolatileJobworkerVolume> volumes = volumeSet.getVolumesList();
      assertEquals(NUM_OF_VOLUMES_PER_JOBWORKER, volumes.size());

      for (VolatileJobworkerVolume volume : volumes) {
        assertEquals(VolatileJobworkerVolume.VolumeState.NORMAL, volume.getState());
        assertEquals(jobworker.getJobworkerDetails().getUuidString(), volume.getJobworkerUuid());
        assertEquals(cluster.getClusterId(), volume.getClusterID());
        assertFalse(volume.isFailed());
      }

      String taskId = "test-task-" + i;
      File taskDir = volumeSet.createTaskDirectory(taskId);
      assertTrue(taskDir.exists());
      assertTrue(taskDir.isDirectory());
      File testFile = new File(taskDir, "test-file.txt");
      assertTrue(testFile.createNewFile());


      boolean cleanupResult = volumeSet.cleanupTask(taskId);
      assertTrue(cleanupResult);
      assertFalse(taskDir.exists());
    }
  }

  @Test
  public void testJobworkerWithOMRestart() throws Exception {
    OzoneManager omToRestart = cluster.getOzoneManager(0, 0);
    cluster.shutdownOzoneManager(omToRestart);
    // simulate OM restart and lose all the jobworker info
    JobworkerNodeStateManager nodeManager = omToRestart.getJobworkerNodemanager().getNodeStateManager();
    for (JobworkerInfo jobworkerInfo : nodeManager.getAllJobworkerInfos()) {
      nodeManager.removeNode(jobworkerInfo.getUuid());
    }

    // Wait to ensure JobWorker notices the OM is gone
    Thread.sleep(10000);
    assertEquals(0, nodeManager.getTotalNodeCount());

    // Verify JobWorkers are still running
    for (JobworkerService jobworker : cluster.getJobworkers()) {
      assertEquals(JobworkerStates.RUNNING,
          jobworker.getJobworkerStateMachine().getContext().getState());
    }

    // Restart the OM
    cluster.restartOzoneManager(omToRestart, true);

    // Verify JobWorkers are registered with the restarted OM
    GenericTestUtils.waitFor(() -> {
      try {
        return nodeManager.getTotalNodeCount() == NUM_OF_JOBWORKERS;
      } catch (Exception e) {
        LOG.error("Error checking jobworker registration status", e);
        return false;
      }
    }, 1000, 60000);

    assertEquals(NUM_OF_JOBWORKERS, nodeManager.getTotalNodeCount());
  }

  private void assertJobworkerRunning(List<JobworkerService> jobworkers) {
    for (JobworkerService jobworker : jobworkers) {
      assertEquals(JobworkerStates.RUNNING,
          jobworker.getJobworkerStateMachine().getContext().getState());
      int expectedEndpoints = NUM_OF_OMS_PER_SERVICE * omServiceIds.size();
      assertEquals(expectedEndpoints,
          jobworker.getJobworkerStateMachine().getConnectionManager().getAllEndpoints().size());
    }
  }
}