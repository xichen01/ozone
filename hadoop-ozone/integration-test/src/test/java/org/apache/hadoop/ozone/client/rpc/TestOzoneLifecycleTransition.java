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
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_TRANSITION_EC_REPLICATION_CONFIG_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ObjectAttributes;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
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
    conf.set(OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY,
        SCMContainerPlacementRandom.class.getName());

    JobWorkerMigrationKeyConfiguration migrationConfiguration =
        conf.getObject(JobWorkerMigrationKeyConfiguration.class);
    migrationConfiguration.setStoragePolicySatisfierIntervalMs(2000);
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
    createRatisKey(volume, keyName, KEY_CONTENT,
        Instant.now().minus(2, ChronoUnit.DAYS));
    volume.getBucket(BUCKET_NAME).setLifecycleConfiguration(createLifecycleConfiguration());

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
    assertKeyContent(keyName, KEY_CONTENT);
    assertMigrationTaskCompleted();
  }

  private void createRatisKey(OzoneVolume volume, String keyName, String content, Instant mtime)
      throws IOException {
    ObjectAttributes attributes = new ObjectAttributes();
    attributes.setMtime(mtime.toEpochMilli());
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    try (OzoneOutputStream output = bucket.createKey(keyName, contentBytes.length,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE), Collections.emptyMap(),
        Collections.emptyMap(), attributes)) {
      output.write(contentBytes);
    }
  }

  private OmLifecycleConfiguration createLifecycleConfiguration() throws IOException {
    OmLCTransition transition = new OmLCTransition.Builder()
        .setDays(1)
        .setStorageClass(OmLCTransition.DEEP_ARCHIVE)
        .build();
    OmLCRule rule = new OmLCRule.Builder()
        .setId("deep-archive-to-ec")
        .setEnabled(true)
        .setFilter(new OmLCFilter.Builder().setPrefix("").build())
        .addAction(transition)
        .build();
    return new OmLifecycleConfiguration.Builder()
        .setVolume(store.getS3Volume().getName())
        .setBucket(BUCKET_NAME)
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

  private void assertKeyContent(String keyName, String expectedContent) {
    try (InputStream input = store.getS3Volume().getBucket(BUCKET_NAME).readKey(keyName)) {
      assertEquals(expectedContent, new String(IOUtils.toByteArray(input), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new AssertionError("Failed to read migrated key " + keyName, e);
    }
  }

  private void assertMigrationTaskCompleted()
      throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        List<MigrationTaskManager.TaskEntry> tasks = cluster.getOMLeader()
            .getMigrationTaskManager().getAllTasks();
        return tasks.size() == 1 && tasks.get(0).getTask().getMigrationStatus()
            == HddsProtos.JobworkerTaskStatus.COMPLETED;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 120000);

    List<MigrationTaskManager.TaskEntry> tasks = cluster.getOMLeader()
        .getMigrationTaskManager().getAllTasks();
    assertEquals(1, tasks.size());
    HddsProtos.JobworkerMigrationKeysTaskProto task = tasks.get(0).getTask();
    assertEquals(1, task.getTotalKeyCount());
    assertEquals(1, task.getMigratedKeyCount());
    assertEquals(0, task.getFailedKeyCount());
    assertTrue(task.getCompleteScanning());
    assertEquals(0, cluster.getOMLeader().getMigrationTaskManager().getAllTransactions().size());
  }
}
