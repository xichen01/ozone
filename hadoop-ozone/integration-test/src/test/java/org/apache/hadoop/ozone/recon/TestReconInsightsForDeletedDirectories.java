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
package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.recon.api.OMDBInsightEndpoint;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Test class to verify the correctness of the insights generated by Recon
 * for Deleted Directories.
 */
public class TestReconInsightsForDeletedDirectories {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReconInsightsForDeletedDirectories.class);

  private static boolean omRatisEnabled = true;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;
  private static String volumeName;
  private static String bucketName;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_DIR_DELETING_SERVICE_INTERVAL, 1000000);
    conf.setInt(OZONE_PATH_DELETING_LIMIT_PER_TASK, 0);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 10000000,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .includeRecon(true)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @AfterEach
  public void cleanup() {
    try {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException ex) {
      fail("Failed to cleanup files.");
    }
  }

  /**
   * Test case for verifying directory deletion and namespace summary updates.
   *
   *      dir1
   *      ├── file1
   *      ├── file2
   *      ├── ...
   *      └── file10
   */
  @Test
  public void testGetDeletedDirectoryInfo()
      throws Exception {

    // Create a directory structure with 10 files in dir1.
    Path dir1 = new Path("/dir1");
    fs.mkdirs(dir1);
    for (int i = 1; i <= 10; i++) {
      Path path1 = new Path(dir1, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(path1)) {
        stream.write(1);
      }
    }

    // Fetch the file table and directory table from Ozone Manager.
    OMMetadataManager ozoneMetadataManagerInstance =
        cluster.getOzoneManager().getMetadataManager();
    Table<String, OmKeyInfo> omFileTable =
        ozoneMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> omDirTable =
        ozoneMetadataManagerInstance.getDirectoryTable();

    // Verify the entries in the Ozone Manager tables.
    assertTableRowCount(omFileTable, 10, false);
    assertTableRowCount(omDirTable, 1, false);

    // Sync data from Ozone Manager to Recon.
    syncDataFromOM();

    // Retrieve tables from Recon's OM-DB.
    ReconOMMetadataManager reconOmMetadataManagerInstance =
        (ReconOMMetadataManager) cluster.getReconServer()
            .getOzoneManagerServiceProvider().getOMMetadataManagerInstance();
    Table<String, OmKeyInfo> reconFileTable =
        reconOmMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> reconDirTable =
        reconOmMetadataManagerInstance.getDirectoryTable();
    Table<String, OmKeyInfo> reconDeletedDirTable =
        reconOmMetadataManagerInstance.getDeletedDirTable();

    // Verify the entries in the Recon tables after sync.
    assertTableRowCount(reconFileTable, 10, true);
    assertTableRowCount(reconDirTable, 1, true);
    assertTableRowCount(reconDeletedDirTable, 0, true);

    // Retrieve the object ID of dir1 from directory table.
    Long directoryObjectId = null;
    try (
        TableIterator<?, ? extends Table.KeyValue<?, OmDirectoryInfo>> iterator
            = reconDirTable.iterator()) {
      if (iterator.hasNext()) {
        directoryObjectId = iterator.next().getValue().getObjectID();
      }
    }

    if (directoryObjectId == null) {
      fail("directoryObjectId is null. Test case cannot proceed.");
    }

    // Retrieve Namespace Summary for dir1 from Recon.
    ReconNamespaceSummaryManagerImpl namespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();
    NSSummary summary =
        namespaceSummaryManager.getNSSummary(directoryObjectId);
    // Assert that the directory dir1 has 10 sub-files and size of 1000 bytes.
    Assert.assertEquals(10, summary.getNumOfFiles());
    Assert.assertEquals(10, summary.getSizeOfFiles());

    // Delete the entire directory dir1.
    fs.delete(dir1, true);
    syncDataFromOM();
    // Check the count of recon directory table and recon deletedDirectory table
    assertTableRowCount(reconDirTable, 0, true);

    assertTableRowCount(reconDeletedDirTable, 1, true);

    // Create an Instance of OMDBInsightEndpoint.
    OzoneStorageContainerManager reconSCM =
        cluster.getReconServer().getReconStorageContainerManager();
    ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();

    OMDBInsightEndpoint omdbInsightEndpoint =
        new OMDBInsightEndpoint(reconSCM, reconOmMetadataManagerInstance,
            mock(GlobalStatsDao.class), reconNamespaceSummaryManager);

    // Fetch the deleted directory info from Recon OmDbInsightEndpoint.
    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse entity =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    // Assert the size of deleted directory is 10.
    Assert.assertEquals(10, entity.getUnreplicatedDataSize());

    // Cleanup the tables.
    cleanupTables();
  }


  /**
   * Directory and File Hierarchy.
   *
   *      dir1
   *      ├── dir2
   *      │   ├── dir3
   *      │   │   ├── file1
   *      │   │   ├── file2
   *      │   │   └── file3
   *
   */
  @Test
  public void testGetDeletedDirectoryInfoForNestedDirectories()
      throws Exception {

    // Create a directory structure with 10 files and 3 nested directories.
    Path path = new Path("/dir1/dir2/dir3");
    fs.mkdirs(path);
    // Create 3 files inside dir3.
    for (int i = 1; i <= 3; i++) {
      Path filePath = new Path(path, "testKey" + i);
      try (FSDataOutputStream stream = fs.create(filePath)) {
        stream.write(1);
      }
    }

    // Fetch the file table and directory table from Ozone Manager.
    OMMetadataManager ozoneMetadataManagerInstance =
        cluster.getOzoneManager().getMetadataManager();
    Table<String, OmKeyInfo> omFileTable =
        ozoneMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> omDirTable =
        ozoneMetadataManagerInstance.getDirectoryTable();

    // Verify the entries in the Ozone Manager tables.
    assertTableRowCount(omFileTable, 3, false);
    assertTableRowCount(omDirTable, 3, false);

    // Sync data from Ozone Manager to Recon.
    syncDataFromOM();

    // Retrieve tables from Recon's OM-DB.
    ReconOMMetadataManager reconOmMetadataManagerInstance =
        (ReconOMMetadataManager) cluster.getReconServer()
            .getOzoneManagerServiceProvider().getOMMetadataManagerInstance();
    Table<String, OmKeyInfo> reconFileTable =
        reconOmMetadataManagerInstance.getKeyTable(getFSOBucketLayout());
    Table<String, OmDirectoryInfo> reconDirTable =
        reconOmMetadataManagerInstance.getDirectoryTable();
    Table<String, OmKeyInfo> reconDeletedDirTable =
        reconOmMetadataManagerInstance.getDeletedDirTable();

    // Verify the entries in the Recon tables after sync.
    assertTableRowCount(reconFileTable, 3, true);
    assertTableRowCount(reconDirTable, 3, true);
    assertTableRowCount(reconDeletedDirTable, 0, true);

    // Create an Instance of OMDBInsightEndpoint.
    OzoneStorageContainerManager reconSCM =
        cluster.getReconServer().getReconStorageContainerManager();
    ReconNamespaceSummaryManagerImpl namespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();

    OMDBInsightEndpoint omdbInsightEndpoint =
        new OMDBInsightEndpoint(reconSCM, reconOmMetadataManagerInstance,
            mock(GlobalStatsDao.class), namespaceSummaryManager);

    // Delete the entire root directory dir1.
    fs.delete(new Path("/dir1/dir2/dir3"), true);
    syncDataFromOM();

    // Verify the entries in the Recon tables after sync.
    assertTableRowCount(reconFileTable, 3, true);
    assertTableRowCount(reconDirTable, 2, true);
    assertTableRowCount(reconDeletedDirTable, 1, true);

    // Fetch the deleted directory info from Recon OmDbInsightEndpoint.
    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse entity =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    // Assert the size of deleted directory is 3.
    Assert.assertEquals(3, entity.getUnreplicatedDataSize());

    // Cleanup the tables.
    cleanupTables();
  }

  /**
   * Directory and File Hierarchy.
   *
   *    largeDirectory
   *    ├── subdir1
   *    │   ├── file1
   *    │   ├── file2
   *    │   ├── ...
   *    │   └── file10
   *    ├── subdir2
   *    │   ├── file1
   *    │   ├── file2
   *    │   ├── ...
   *    │   └── file10
   *    ├── ...
   *    └── subdir10
   *        ├── file1
   *        ├── file2
   *        ├── ...
   *        └── file10
   */
  @Test
  public void testGetDeletedDirectoryInfoWithMultipleSubdirectories()
      throws Exception {
    int numSubdirectories = 10;
    int filesPerSubdirectory = 10;

    // Create a large directory structure
    Path rootDir = new Path("/largeDirectory");
    createLargeDirectory(rootDir, numSubdirectories, filesPerSubdirectory);

    // Delete the large directory
    fs.delete(rootDir, true);

    // Verify that the directory is deleted
    Assertions.assertFalse(fs.exists(rootDir), "Directory was not deleted");

    // Sync data from Ozone Manager to Recon.
    syncDataFromOM();

    // Fetch the deleted directory info from Recon OmDbInsightEndpoint.
    OzoneStorageContainerManager reconSCM =
        cluster.getReconServer().getReconStorageContainerManager();
    ReconNamespaceSummaryManagerImpl namespaceSummaryManager =
        (ReconNamespaceSummaryManagerImpl) cluster.getReconServer()
            .getReconNamespaceSummaryManager();
    ReconOMMetadataManager reconOmMetadataManagerInstance =
        (ReconOMMetadataManager) cluster.getReconServer()
            .getOzoneManagerServiceProvider().getOMMetadataManagerInstance();
    OMDBInsightEndpoint omdbInsightEndpoint =
        new OMDBInsightEndpoint(reconSCM, reconOmMetadataManagerInstance,
            mock(GlobalStatsDao.class), namespaceSummaryManager);
    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse entity =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    // Assert the size of deleted directory is 100.
    Assert.assertEquals(100, entity.getUnreplicatedDataSize());

    // Cleanup the tables.
    cleanupTables();
  }

  private void createLargeDirectory(Path dir, int numSubdirs,
                                    int numFilesPerSubdir) throws IOException {
    fs.mkdirs(dir);
    for (int i = 1; i <= numSubdirs; i++) {
      Path subDir = new Path(dir, "subdir" + i);
      fs.mkdirs(subDir);
      for (int j = 1; j <= numFilesPerSubdir; j++) {
        Path filePath = new Path(subDir, "file" + j);
        try (FSDataOutputStream stream = fs.create(filePath)) {
          stream.write(1);
        }
      }
    }
  }

  /**
   * Cleans up the tables by removing all entries from the deleted directory,
   * file, and directory tables within the Ozone metadata manager. This method
   * iterates through the tables and removes all entries from each table.
   */
  private void cleanupTables() throws IOException {
    OMMetadataManager metadataManager =
        cluster.getOzoneManager().getMetadataManager();

    try (TableIterator<?, ?> it = metadataManager.getDeletedDirTable()
        .iterator()) {
      removeAllFromDB(it);
    }
    try (TableIterator<?, ?> it = metadataManager.getFileTable().iterator()) {
      removeAllFromDB(it);
    }
    try (TableIterator<?, ?> it = metadataManager.getDirectoryTable()
        .iterator()) {
      removeAllFromDB(it);
    }
  }

  private static void removeAllFromDB(TableIterator<?, ?> iterator)
      throws IOException {
    while (iterator.hasNext()) {
      iterator.next();
      iterator.removeFromDB();
    }
  }

  /**
   * Asserts the expected row count in a specified table. This method uses a
   * timed wait mechanism to repeatedly check the row count until the expected
   * count is achieved or a timeout occurs.
   *
   * @param table   The table for which to assert the row count.
   * @param expectedCount   The expected row count.
   * @param isRecon A boolean indicating whether the table is a Recon table.
   */
  private void assertTableRowCount(Table<String, ?> table, int expectedCount,
                                   boolean isRecon)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> assertTableRowCount(expectedCount, table, isRecon), 1000,
        120000); // 2 minutes
  }

  private boolean assertTableRowCount(int expectedCount,
                                      Table<String, ?> table, boolean isRecon) {
    long count = 0L;
    try {
      if (isRecon) {
        count = cluster.getReconServer().getOzoneManagerServiceProvider()
            .getOMMetadataManagerInstance().countRowsInTable(table);
      } else {
        count = cluster.getOzoneManager().getMetadataManager()
            .countRowsInTable(table);
      }
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("Test failed with: " + ex);
    }
    return count == expectedCount;
  }

  private void syncDataFromOM() {
    // Sync data from Ozone Manager to Recon.
    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();
    impl.syncDataFromOM();
  }


  private static BucketLayout getFSOBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

}
