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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ObjectAttributes;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.JobworkerService;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLCTransition;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.service.StoragePolicySatisfierService;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Slow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** End-to-end coverage for Object bucket lifecycle to EC key migration. */
@Slow("HDDS-16402")
@Timeout(300)
public class TestOzoneLifecycleTransition {

  private static final String OM_SERVICE_ID = "om-service-lifecycle-transition";
  private static final String KEY_CONTENT = "lifecycle migration content";
  private static final String BUCKET_NAME = "lifecycle-transition-bucket";
  private static OzoneConfiguration conf;
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient ozoneClient;
  private static ObjectStore store;

  @BeforeAll
  public static void initialize() throws Exception {
    conf = new OzoneConfiguration();
    configureCluster();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setNumOfOzoneManagers(3)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfJobworkers(1)
        .setNumDatanodes(10)
        .build();
    cluster.waitForClusterToBeReady();

    ozoneClient = OzoneClientFactory.getRpcClient(conf);
    store = ozoneClient.getObjectStore();
  }

  @AfterAll
  public static void shutdownCluster() throws IOException {
    IOUtils.closeQuietly(ozoneClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void configureCluster() {
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.setBoolean(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, true);
    conf.setTimeDuration(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, 2, TimeUnit.SECONDS);
    conf.set(OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG,
        OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG_DEFAULT);

    JobWorkerMigrationKeyConfiguration migrationConfiguration =
        conf.getObject(JobWorkerMigrationKeyConfiguration.class);
    migrationConfiguration.setStoragePolicySatisfierIntervalMs(2000);
    migrationConfiguration.setStoragePolicySatisfierLeaderReadyWaitTimeMs(2000);
    migrationConfiguration.setThreadPoolSize(1);

    JobworkerConfiguration jobworkerConfiguration = conf.getObject(JobworkerConfiguration.class);
    jobworkerConfiguration.setHeartbeatInterval(java.time.Duration.ofSeconds(2));
    conf.setFromObject(migrationConfiguration);
    conf.setFromObject(jobworkerConfiguration);
  }

  @Test
  public void testLifecycleTransitionEndToEnd() throws Exception {
    OzoneVolume volume = store.getS3Volume();
    volume.createBucket(BUCKET_NAME,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE).build());

    String keyName = "transition-to-ec";
    createRatisKey(volume, BUCKET_NAME, keyName, KEY_CONTENT,
        Instant.now().minus(2, ChronoUnit.DAYS));
    volume.getBucket(BUCKET_NAME).setLifecycleConfiguration(
        createLifecycleConfiguration(BUCKET_NAME, "deep-archive-to-ec"));

    GenericTestUtils.waitFor((BooleanSupplier) () -> {
      try {
        OmKeyInfo keyInfo = getKeyInfo(BUCKET_NAME, keyName);
        return keyInfo != null && keyInfo.getReplicationConfig().equals(expectedEcConfig());
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);

    OmKeyInfo migratedKey = getKeyInfo(BUCKET_NAME, keyName);
    assertNotNull(migratedKey);
    assertEquals(expectedEcConfig(), migratedKey.getReplicationConfig());
    assertKeyContent(BUCKET_NAME, keyName, KEY_CONTENT);
    assertMigrationTaskCompleted(BUCKET_NAME);
  }

  @Test
  public void testStoragePolicySatisfierServiceDuringHATransfer() throws Exception {
    OzoneVolume volume = store.getS3Volume();
    String bucketName = "lifecycle-ha-transfer";
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE).build());

    createRatisKey(volume, bucketName, "before-transfer", KEY_CONTENT,
        Instant.now().minus(2, ChronoUnit.DAYS));
    volume.getBucket(bucketName).setLifecycleConfiguration(
        createLifecycleConfiguration(bucketName, "before-transfer-rule"));

    GenericTestUtils.waitFor(() -> isReplicationConfig(bucketName, "before-transfer",
        expectedEcConfig()), 1000, 120000);

    OzoneManager oldLeader = cluster.waitForLeaderOM();
    OzoneManager newLeader = null;
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      if (om != oldLeader) {
        newLeader = om;
        assertFalse(om.getKeyManager().getStoragePolicySatisfierService().shouldRun());
      }
    }
    assertNotNull(newLeader);
    assertTrue(oldLeader.getKeyManager().getStoragePolicySatisfierService().shouldRun());

    oldLeader.transferLeadership(newLeader.getOMNodeId());
    OzoneManager electedLeader = cluster.waitForLeaderOM();
    assertNotNull(electedLeader);
    assertNotEquals(oldLeader.getOMNodeId(), electedLeader.getOMNodeId());
    assertTrue(electedLeader.isLeaderReady());

    StoragePolicySatisfierService oldService =
        oldLeader.getKeyManager().getStoragePolicySatisfierService();
    GenericTestUtils.waitFor(() -> !oldService.shouldRun(), 500, 30000);
    assertEquals(0, oldService.getRunningTaskCount());

    StoragePolicySatisfierService newService =
        electedLeader.getKeyManager().getStoragePolicySatisfierService();
    GenericTestUtils.waitFor(newService::shouldRun, 500, 30000);

    String transferredKey = "after-transfer";
    createRatisKey(volume, bucketName, transferredKey, KEY_CONTENT,
        Instant.now().minus(2, ChronoUnit.DAYS));
    volume.getBucket(bucketName).setLifecycleConfiguration(
        createLifecycleConfiguration(bucketName, "after-transfer-rule"));

    GenericTestUtils.waitFor(() -> isReplicationConfig(bucketName, transferredKey,
        expectedEcConfig()), 1000, 120000);
    assertKeyContent(bucketName, transferredKey, KEY_CONTENT);
    assertMigrationTasksCompleted(bucketName, 2);
  }

  @Test
  public void testCancelMigrationTask() throws Exception {
    OzoneVolume volume = store.getS3Volume();
    String bucketName = "lifecycle-cancel";
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE).build());

    List<String> keyNames = new ArrayList<>();
    Instant oldMtime = Instant.now().minus(2, ChronoUnit.DAYS);
    for (int i = 0; i < 5; i++) {
      String keyName = "cancel-" + i;
      createRatisKey(volume, bucketName, keyName, KEY_CONTENT, oldMtime);
      keyNames.add(keyName);
    }
    volume.getBucket(bucketName).setLifecycleConfiguration(
        createLifecycleConfiguration(bucketName, "cancel-rule"));

    JobworkerCommandDelayInjector injector = new JobworkerCommandDelayInjector();
    for (JobworkerService jobworker : cluster.getJobworkers()) {
      jobworker.getJobworkerStateMachine().setHandlerInjector(
          JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type.migrateKeyCommand,
          injector);
    }

    GenericTestUtils.waitFor(() -> {
      try {
        List<MigrationTaskManager.TaskEntry> tasks = cluster.waitForLeaderOM()
            .getMigrationTaskManager().listTask(volume.getName(), bucketName);
        return tasks.size() == 1 && tasks.get(0).getTask().getMigrationStatus()
            == HddsProtos.JobworkerTaskStatus.EXECUTING;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 60000);

    String taskKey = cluster.waitForLeaderOM().getMigrationTaskManager()
        .listTask(volume.getName(), bucketName).get(0).getTaskKey();
    store.getClientProxy().getOzoneManagerClient().cancelMigrationKeyTask(taskKey);

    GenericTestUtils.waitFor(() -> {
      try {
        HddsProtos.JobworkerMigrationKeysTaskProto task =
            cluster.waitForLeaderOM().getMigrationTaskManager().getTask(taskKey);
        return task != null && task.getMigrationStatus() == HddsProtos.JobworkerTaskStatus.CANCELED;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 60000);

    assertTrue(cluster.waitForLeaderOM().getMigrationTaskManager()
        .listTransactionsForTask(taskKey, null, 1).isEmpty());
    for (String keyName : keyNames) {
      assertEquals(ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
          getKeyInfo(bucketName, keyName).getReplicationConfig());
    }

    injector.resume();
    for (JobworkerService jobworker : cluster.getJobworkers()) {
      jobworker.getJobworkerStateMachine().setHandlerInjector(
          JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type.migrateKeyCommand, null);
    }

    GenericTestUtils.waitFor((BooleanSupplier) () -> {
      try {
        return cluster.waitForLeaderOM().getMigrationTaskManager()
            .listTask(volume.getName(), bucketName).stream()
            .anyMatch(task -> task.getTask().getMigrationStatus()
                == HddsProtos.JobworkerTaskStatus.COMPLETED);
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);

    assertCanceledAndCompletedMigrationTasks(bucketName);
    for (String keyName : keyNames) {
      assertEquals(expectedEcConfig(), getKeyInfo(bucketName, keyName).getReplicationConfig());
    }
  }

  private void createRatisKey(OzoneVolume volume, String bucketName, String keyName,
      String content, Instant mtime)
      throws IOException {
    ObjectAttributes attributes = new ObjectAttributes();
    attributes.setMtime(mtime.toEpochMilli());
    OzoneBucket bucket = volume.getBucket(bucketName);
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    try (OzoneOutputStream output = bucket.createKey(keyName, contentBytes.length,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE), Collections.emptyMap(),
        Collections.emptyMap(), attributes)) {
      output.write(contentBytes);
    }
  }

  private OmLifecycleConfiguration createLifecycleConfiguration(String bucketName, String ruleId)
      throws IOException {
    OmLCTransition transition = new OmLCTransition.Builder()
        .setDays(1)
        .setStorageClass(OmLCTransition.DEEP_ARCHIVE)
        .build();
    OmLCRule rule = new OmLCRule.Builder()
        .setId(ruleId)
        .setEnabled(true)
        .setFilter(new OmLCFilter.Builder().setPrefix("").build())
        .addAction(transition)
        .build();
    return new OmLifecycleConfiguration.Builder()
        .setVolume(store.getS3Volume().getName())
        .setBucket(bucketName)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setCreationTime(System.currentTimeMillis())
        .setRules(Collections.singletonList(rule))
        .build();
  }

  private OmKeyInfo getKeyInfo(String bucketName, String keyName) throws IOException {
    OzoneManager ozoneManager = cluster.getOMLeader();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(store.getS3Volume().getName())
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    try {
      return ozoneManager.lookupKey(keyArgs);
    } catch (OMException e) {
      if (e.getResult() == OMException.ResultCodes.KEY_NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  private ECReplicationConfig expectedEcConfig() {
    return new ECReplicationConfig(conf.get(OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG,
        OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG_DEFAULT));
  }

  private void assertKeyContent(String bucketName, String keyName, String expectedContent) {
    try (InputStream input = store.getS3Volume().getBucket(bucketName).readKey(keyName)) {
      assertEquals(expectedContent, new String(IOUtils.toByteArray(input), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new AssertionError("Failed to read migrated key " + keyName, e);
    }
  }

  private void assertMigrationTaskCompleted(String bucketName)
      throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        List<MigrationTaskManager.TaskEntry> tasks = cluster.getOMLeader()
            .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
        return tasks.size() == 1 && tasks.get(0).getTask().getMigrationStatus()
            == HddsProtos.JobworkerTaskStatus.COMPLETED;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 120000);

    List<MigrationTaskManager.TaskEntry> tasks = cluster.getOMLeader()
        .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
    assertEquals(1, tasks.size());
    HddsProtos.JobworkerMigrationKeysTaskProto task = tasks.get(0).getTask();
    assertEquals(1, task.getTotalKeyCount());
    assertEquals(1, task.getMigratedKeyCount());
    assertEquals(0, task.getFailedKeyCount());
    assertTrue(task.getCompleteScanning());
    assertEquals(0, cluster.getOMLeader().getMigrationTaskManager().getAllTransactions().size());
  }

  private boolean isReplicationConfig(String bucketName, String keyName,
      ReplicationConfig expected) {
    try {
      OmKeyInfo keyInfo = getKeyInfo(bucketName, keyName);
      return keyInfo != null && expected.equals(keyInfo.getReplicationConfig());
    } catch (IOException e) {
      return false;
    }
  }

  private void assertMigrationTasksCompleted(String bucketName, int expectedTaskCount)
      throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        List<MigrationTaskManager.TaskEntry> tasks = cluster.waitForLeaderOM()
            .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
        return tasks.size() == expectedTaskCount && tasks.stream().allMatch(task ->
            task.getTask().getMigrationStatus() == HddsProtos.JobworkerTaskStatus.COMPLETED);
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);

    List<MigrationTaskManager.TaskEntry> tasks = cluster.waitForLeaderOM()
        .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
    assertEquals(expectedTaskCount, tasks.size());
    for (MigrationTaskManager.TaskEntry taskEntry : tasks) {
      HddsProtos.JobworkerMigrationKeysTaskProto task = taskEntry.getTask();
      assertEquals(task.getTotalKeyCount(), task.getMigratedKeyCount());
      assertEquals(0, task.getFailedKeyCount());
    }
    assertEquals(0, cluster.waitForLeaderOM().getMigrationTaskManager()
        .getAllTransactions().size());
  }

  private void assertCanceledAndCompletedMigrationTasks(String bucketName)
      throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        List<MigrationTaskManager.TaskEntry> tasks = cluster.waitForLeaderOM()
            .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
        boolean canceled = tasks.stream().anyMatch(task ->
            task.getTask().getMigrationStatus() == HddsProtos.JobworkerTaskStatus.CANCELED);
        boolean completed = tasks.stream().anyMatch(task ->
            task.getTask().getMigrationStatus() == HddsProtos.JobworkerTaskStatus.COMPLETED);
        return tasks.size() == 2 && canceled && completed;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);

    List<MigrationTaskManager.TaskEntry> tasks = cluster.waitForLeaderOM()
        .getMigrationTaskManager().listTask(store.getS3Volume().getName(), bucketName);
    assertEquals(2, tasks.size());
    for (MigrationTaskManager.TaskEntry taskEntry : tasks) {
      HddsProtos.JobworkerMigrationKeysTaskProto task = taskEntry.getTask();
      if (task.getMigrationStatus() == HddsProtos.JobworkerTaskStatus.CANCELED) {
        assertEquals(0, task.getMigratedKeyCount());
      } else {
        assertEquals(HddsProtos.JobworkerTaskStatus.COMPLETED, task.getMigrationStatus());
        assertEquals(5, task.getTotalKeyCount());
        assertEquals(5, task.getMigratedKeyCount());
        assertEquals(0, task.getFailedKeyCount());
        assertTrue(task.getCompleteScanning());
      }
    }
    assertEquals(0, cluster.waitForLeaderOM().getMigrationTaskManager()
        .getAllTransactions().size());
  }

  private static class JobworkerCommandDelayInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    JobworkerCommandDelayInjector() {
      init();
    }

    @Override
    public void init() {
      ready = new CountDownLatch(1);
      wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Command injector interrupted", e);
      }
    }

    @Override
    public void resume() throws IOException {
      try {
        ready.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Command injector interrupted", e);
      }
      wait.countDown();
    }
  }
}
