package org.apache.hadoop.ozone.om.jobworker.command;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationKeyResult;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerTaskStatus;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerMigrateKeyCommand;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for {@link MigrateKeyCommandListener}.
 */
public class TestMigrateKeyCommandListener {
  private static final String OM_SERVICE_ID = "om-service-1";
  private static final int DEFAULT_KEY_COUNT = 4;

  @TempDir
  private static File tempDir;

  private static OzoneManager ozoneManager;
  private static OMMetadataManager metadataManager;
  private static JobworkerNodeManager jobworkerNodeManager;
  private static OMJobworkerCommandManager commandManager;
  private static MigrateKeyCommandListener listener;
  private static OzoneConfiguration conf;
  private static UUID jobworker1Uuid;
  private static MigrationTaskManager migrationTaskManager;
  private static JobworkerDetails mockJobworker1Details;
  private static int maxRetryCount;
  private String volumeName;
  private String bucketName;
  private List<MigrationKeyProto> migrationKeys;
  private String taskKey;

  @BeforeAll
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());

    OmTestManagers omTestManagers = new OmTestManagers(conf);
    ozoneManager = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
    migrationTaskManager = ozoneManager.getMigrationTaskManager();
    jobworkerNodeManager = ozoneManager.getJobworkerNodemanager();
    commandManager = ozoneManager.getOMJobworkerCommandManager();
    listener = (MigrateKeyCommandListener) commandManager.getListener(
        Type.migrateKeyCommand);
    jobworker1Uuid = UUID.randomUUID();
    mockJobworker1Details = MockJobworkerDetails.createJobworkerDetails(jobworker1Uuid.toString());
    ozoneManager.getJobworkerNodemanager().registerJobworker(mockJobworker1Details);
    maxRetryCount = conf.getObject(JobWorkerMigrationKeyConfiguration.class).getCommandMaxRetryCount();
  }

  @BeforeEach
  public void cleanupTables() throws Exception {
    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTaskProto>>
        statusIter = metadataManager.getJobworkerMigrationKeysTaskTable().iterator()) {
      while (statusIter.hasNext()) {
        String key = statusIter.next().getKey();
        metadataManager.getJobworkerMigrationKeysTaskTable().delete(key);
      }
    }

    try (TableIterator<String, ? extends Table.KeyValue<String, JobworkerMigrationKeysTxProto>>
        taskIter = metadataManager.getJobworkerMigrationKeysTxTable().iterator()) {
      while (taskIter.hasNext()) {
        String key = taskIter.next().getKey();
        metadataManager.getJobworkerMigrationKeysTxTable().delete(key);
      }
    }
    // clear remaining Jobworker command
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);

    // Generate random test data
    volumeName = "/volume" + RandomUtils.nextInt();
    bucketName = "/bucket" + RandomUtils.nextInt();
    migrationKeys = new ArrayList<>();
    for (int i = 0; i < DEFAULT_KEY_COUNT; i++) {
      migrationKeys.add(MigrationKeyProto.newBuilder()
          .setKey("key" + UUID.randomUUID().toString().substring(0, 8))
          .setUpdateID(RandomUtils.nextLong())
          .build());
    }
    taskKey = volumeName + bucketName + "/log" + UUID.randomUUID().toString().substring(0, 8);
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (ozoneManager != null) {
      ozoneManager.stop();
    }
    if (commandManager != null) {
      commandManager.close();
    }
  }


  /**
   * Verifies that the retry command matches the original command with incremented retry count.
   *
   * @param originalCommand Original migration command
   * @param retryCommand    Retry command to verify
   * @param expectedKeys    Expected keys in retry command
   */
  private void verifyRetryCommand(OMJobworkerMigrateKeyCommand originalCommand,
      OMJobworkerMigrateKeyCommand retryCommand, List<MigrationKeyProto> expectedKeys) {
    assertEquals(originalCommand.getTxId(), retryCommand.getTxId());
    assertEquals(originalCommand.getVolume(), retryCommand.getVolume());
    assertEquals(originalCommand.getBucket(), retryCommand.getBucket());
    assertEquals(originalCommand.getStoragePolicy(), retryCommand.getStoragePolicy());
    assertEquals(expectedKeys, retryCommand.getMigrationKeys());
    assertEquals(originalCommand.getTaskKey(), retryCommand.getTaskKey());
    assertEquals(originalCommand.getRetryCount() + 1, retryCommand.getRetryCount());
  }

  @Test
  public void testSuccessfulMigration() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);
    assertTrue(migrationTaskManager.isTaskExists(taskKey));
    assertTrue(migrationTaskManager.isTransactionExists(migrationTxKey));

    // All the keys migrated successfully
    CommandStatus status = createSuccessfulCommandStatus(cmdId, migrationKeys);
    commandManager.processStatusUpdate(mockJobworker1Details, status);

    // Assert
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertEquals(0, migrationTask.getFailedKeyCount());
    assertEquals(migrationKeys.size(), migrationTask.getMigratedKeyCount());
    assertFalse(migrationTaskManager.isTransactionExists(migrationTxKey));
  }

  @Test
  public void testCommandFailedWithRetry() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create failed command status with retryable error
    CommandStatus status = createCommandStatus(cmdId, migrationKeys, CommandStatus.Status.FAILED,
        CommandResultCode.COMMAND_EXPIRED, Collections.emptyMap());
    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    commandManager.processStatusUpdate(mockJobworker1Details, status);

    // Assert task status
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertTrue(migrationTaskManager.isTransactionExists(migrationTxKey));
    assertEquals(JobworkerTaskStatus.PENDING, migrationTask.getMigrationStatus());

    // Verify retry command
    List<OMJobworkerCommand> retryCommands = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertNotNull(retryCommands);
    assertEquals(1, retryCommands.size());
    verifyRetryCommand(command, (OMJobworkerMigrateKeyCommand) retryCommands.get(0), migrationKeys);
  }

  @Test
  public void testCommandFailedWithoutRetry() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create failed command status with non-retryable error
    CommandStatus status = createCommandStatus(cmdId, migrationKeys, CommandStatus.Status.FAILED,
        CommandResultCode.BUCKET_NOT_FOUND, Collections.emptyMap());
    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    commandManager.processStatusUpdate(mockJobworker1Details, status);

    // Assert
    List<OMJobworkerCommand> retryCommands = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertTrue(retryCommands.isEmpty());
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertEquals(0, migrationTask.getMigratedKeyCount());
    assertEquals(migrationKeys.size(), migrationTask.getFailedKeyCount());
    assertFalse(migrationTaskManager.isTransactionExists(migrationTxKey));
  }

  @Test
  public void testPartialSuccessfulMigration() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create partial success status1: 2 success, 1 retryable error, 1 non-retryable error
    Map<String, CommandResultCode> results1 = new HashMap<>();
    results1.put(migrationKeys.get(0).getKey(), CommandResultCode.SUCCESS);
    results1.put(migrationKeys.get(1).getKey(), CommandResultCode.IO_TIMEOUT); // retryable
    results1.put(migrationKeys.get(2).getKey(), CommandResultCode.IO_TIMEOUT); // retryable
    results1.put(migrationKeys.get(3).getKey(), CommandResultCode.KEY_NOT_FOUND);
    
    CommandStatus status1 = createCommandStatus(cmdId, migrationKeys, CommandStatus.Status.SUCCEEDED, null, results1);
    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    commandManager.processStatusUpdate(mockJobworker1Details, status1);

    // Assert task status1 after first attempt
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertTrue(migrationTaskManager.isTransactionExists(migrationTxKey));
    assertEquals(JobworkerTaskStatus.PENDING, migrationTask.getMigrationStatus());

    // Verify first retry command
    List<OMJobworkerCommand> retryCommands1 = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertNotNull(retryCommands1);
    assertEquals(1, retryCommands1.size());
    OMJobworkerMigrateKeyCommand retryCommand1 = (OMJobworkerMigrateKeyCommand) retryCommands1.get(0);
    verifyRetryCommand(command, retryCommand1,
        Arrays.asList(migrationKeys.get(1), migrationKeys.get(2)));
    assertEquals(2, retryCommand1.getMigrationKeysCount());
    assertEquals(1, retryCommand1.getRetryCount());

    // Retry again
    Map<String, CommandResultCode> results2 = new HashMap<>();
    results2.put(migrationKeys.get(1).getKey(), CommandResultCode.SUCCESS);
    results2.put(migrationKeys.get(2).getKey(), CommandResultCode.IO_TIMEOUT); // retryable
    CommandStatus status2 = createCommandStatus(retryCommand1.getId(), Arrays.asList(
            migrationKeys.get(1), migrationKeys.get(2)),
        CommandStatus.Status.SUCCEEDED, null, results2);
    commandManager.processStatusUpdate(mockJobworker1Details, status2);
    List<OMJobworkerCommand> retryCommands2 = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertNotNull(retryCommands2);
    assertEquals(1, retryCommands2.size());
    OMJobworkerMigrateKeyCommand retryCommand2 = (OMJobworkerMigrateKeyCommand) retryCommands2.get(0);
    verifyRetryCommand(retryCommand1, retryCommand2,
        Collections.singletonList(migrationKeys.get(2)));
    assertEquals(2, retryCommand2.getRetryCount());
  }

  @Test
  public void testPartialSuccessfulMigrationWithMaxRetryReached() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);
    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, maxRetryCount);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create partial success status: 2 success, 2 retryable errors
    Map<String, CommandResultCode> results = new HashMap<>();
    results.put(migrationKeys.get(0).getKey(), CommandResultCode.SUCCESS);
    results.put(migrationKeys.get(1).getKey(), CommandResultCode.KEY_GENERATION_MISMATCH);
    results.put(migrationKeys.get(2).getKey(), CommandResultCode.IO_TIMEOUT); // retryable but max retry reached
    results.put(migrationKeys.get(3).getKey(), CommandResultCode.IO_TIMEOUT); // retryable but max retry reached
    
    CommandStatus status = createCommandStatus(cmdId, migrationKeys, CommandStatus.Status.SUCCEEDED, null, results);
    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    commandManager.processStatusUpdate(mockJobworker1Details, status);

    // Assert task status after processing
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertFalse(migrationTaskManager.isTransactionExists(migrationTxKey));
    assertEquals(1, migrationTask.getMigratedKeyCount());
    assertEquals(3, migrationTask.getFailedKeyCount());

    // Verify no retry command is generated since max retry times reached
    List<OMJobworkerCommand> retryCommands = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertTrue(retryCommands.isEmpty());
  }

  @Test
  public void testCommandTimeout() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    commandManager.sendCommand(jobworker1Uuid, command);

    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);

    // Simulate timeout
    listener.onStatusUpdateTimeout(new JobworkerCommandInfo(command, jobworker1Uuid, CommandStatus.Status.PENDING),
        jobworker1Uuid);

    // Assert task status
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertTrue(migrationTaskManager.isTransactionExists(migrationTxKey));
    List<OMJobworkerCommand> retryCommands = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertNotNull(retryCommands);
    // Timeout do not send retry command
    assertTrue(retryCommands.isEmpty());
  }

  @Test
  public void testExceedMaxRetryTimes() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, maxRetryCount);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create failed command status with retryable error but max retry reached
    CommandStatus status = createCommandStatus(cmdId, migrationKeys, CommandStatus.Status.FAILED,
        CommandResultCode.COMMAND_EXPIRED, Collections.emptyMap());
    commandManager.processStatusUpdate(mockJobworker1Details, status);

    // Assert
    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    assertNotNull(migrationTask);
    assertEquals(0, migrationTask.getMigratedKeyCount());
    assertEquals(migrationKeys.size(), migrationTask.getFailedKeyCount());
    assertFalse(migrationTaskManager.isTransactionExists(migrationTxKey));
  }

  @Test
  public void testInvalidMigrationResults() throws Exception {
    // Prepare
    long txId = RandomUtils.nextLong();
    String migrationTxKey = prepareTestData(migrationKeys, taskKey, txId);

    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create command status with mismatched result count
    List<MigrationKeyProto> lessKeys = Arrays.asList(migrationKeys.get(0), migrationKeys.get(1));
    CommandStatus status = createCommandStatus(cmdId, lessKeys, CommandStatus.Status.SUCCEEDED, null,
        lessKeys.stream().collect(Collectors.toMap(MigrationKeyProto::getKey, key -> CommandResultCode.SUCCESS)));
    // Clear any existing commands in the queue
    jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertDoesNotThrow(() -> commandManager.processStatusUpdate(mockJobworker1Details, status));

    JobworkerMigrationKeysTaskProto migrationTask = listener.getTaskStatus(taskKey);
    List<OMJobworkerCommand> retryCommands = jobworkerNodeManager.pollJobworkerCommand(jobworker1Uuid);
    assertEquals(0, retryCommands.size());
    assertNotNull(migrationTask);
    assertEquals(0, migrationTask.getMigratedKeyCount());
    assertEquals(migrationKeys.size(), migrationTask.getFailedKeyCount());
    assertFalse(migrationTaskManager.isTransactionExists(migrationTxKey));
  }

  @Test
  public void testTaskNotFound() throws Exception {
    // Prepare command without inserting task
    long txId = RandomUtils.nextLong();
    OMJobworkerMigrateKeyCommand command = new OMJobworkerMigrateKeyCommand(txId,
        volumeName, bucketName, OzoneStoragePolicy.COLD, migrationKeys, null, taskKey, 0);
    long cmdId = commandManager.sendCommand(jobworker1Uuid, command);

    // Create successful command status
    CommandStatus status = createSuccessfulCommandStatus(cmdId, migrationKeys);
    assertDoesNotThrow(() -> commandManager.processStatusUpdate(mockJobworker1Details, status));
    assertFalse(migrationTaskManager.isTaskExists(taskKey));
  }

  private CommandStatus createSuccessfulCommandStatus(long cmdId, List<MigrationKeyProto> keys) {
    Map<String, CommandResultCode> map = new HashMap<>();
    for (MigrationKeyProto key : keys) {
      map.put(key.getKey(), CommandResultCode.SUCCESS);
    }
    return createCommandStatus(cmdId, keys, Status.SUCCEEDED, null, map);
  }

  private CommandStatus createCommandStatus(long cmdId, List<MigrationKeyProto> keys,
      CommandStatus.Status status, CommandResultCode resultCode,  Map<String, CommandResultCode> keyResults) {

    MigrationResultsProto.Builder resultsBuilder = MigrationResultsProto.newBuilder();
    int successfulCount = 0;
    
    for (MigrationKeyProto key : keys) {
      CommandResultCode keyMigrationResultCode = keyResults.getOrDefault(key.getKey(), CommandResultCode.SUCCESS);
      resultsBuilder.addResults(MigrationKeyResult.newBuilder()
          .setKeyName(key.getKey())
          .setResultCode(keyMigrationResultCode)
          .build());
      if (keyMigrationResultCode == CommandResultCode.SUCCESS) {
        successfulCount++;
      }
    }
    resultsBuilder.setSuccessfulKeyCount(successfulCount);

    CommandExecutionResultsProto executionResults = CommandExecutionResultsProto.newBuilder()
        .setMigrationResults(resultsBuilder.build())
        .build();

    CommandStatus.Builder builder = CommandStatus.newBuilder()
        .setOmServiceId(OM_SERVICE_ID)
        .setCmdId(cmdId)
        .setType(Type.migrateKeyCommand)
        .setStatus(status)
        .setExecutionResults(executionResults);
    if (resultCode != null) {
      builder.setResultCode(resultCode);
    }
    return builder.build();
  }

  private String prepareTestData(List<MigrationKeyProto> keys,
      String task, long txId) throws Exception {
    String migrationTxKey = MigrationTaskManager.getTransactionKey(taskKey, txId);
    long updateTime = System.currentTimeMillis();
    insertMigrationTask(task, keys, updateTime);
    insertMigrationTransaction(migrationTxKey, task, volumeName, bucketName, keys);
    return migrationTxKey;
  }

  private void insertMigrationTask(String task, List<MigrationKeyProto> keys, long updateTime) throws Exception {
    JobworkerMigrationKeysTaskProto statusEntry = JobworkerMigrationKeysTaskProto.newBuilder()
        .setMigrationStatus(JobworkerTaskStatus.PENDING)
        .setTotalKeyCount(keys.size())
        .setMigratedKeyCount(0)
        .setFailedKeyCount(0)
        .setStartTime(updateTime)
        .setLastUpdateTime(updateTime)
        .setCompleteScanning(false)
        .build();

    metadataManager.getJobworkerMigrationKeysTaskTable().put(task, statusEntry);
  }

  private void insertMigrationTransaction(String migrationTxKey, String task,
      String volume, String bucket, List<MigrationKeyProto> keys) throws Exception {
    JobworkerMigrationKeysTxProto migrationKeysTx = JobworkerMigrationKeysTxProto.newBuilder()
        .setTxId(123L)
        .setVolume(volume)
        .setBucket(bucket)
        .setStoragePolicy(OzoneStoragePolicy.toProto(OzoneStoragePolicy.COLD))
        .addAllMigrationKeys(keys)
        .setTaskKey(task)
        .build();

    metadataManager.getJobworkerMigrationKeysTxTable().put(migrationTxKey, migrationKeysTx);
  }
}
