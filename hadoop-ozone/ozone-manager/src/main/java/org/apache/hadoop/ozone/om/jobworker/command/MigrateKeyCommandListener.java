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
 */

package org.apache.hadoop.ozone.om.jobworker.command;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationKeyResult;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTaskProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerMigrateKeyCommand;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.jobworker.MigrationTaskManager;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerInfo;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command listener for key migration operations.
 * Handles the lifecycle events of key migration commands.
 */
public class MigrateKeyCommandListener implements JobworkerCommandListener {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateKeyCommandListener.class);

  private final OzoneManager ozoneManager;
  private final MigrationTaskManager taskManager;
  private final int maxRetryCount;
  private final JobworkerNodeManager jobworkerNodeManager;
  private final OMJobworkerCommandManager jobworkerCommandManager;

  /**
   * Constructor for MigrateKeyCommandListener.
   */
  public MigrateKeyCommandListener(OzoneManager ozoneManager, OzoneConfiguration configuration) {
    this.ozoneManager = ozoneManager;
    this.taskManager = ozoneManager.getMigrationTaskManager();
    JobWorkerMigrationKeyConfiguration jwConf = configuration.getObject(JobWorkerMigrationKeyConfiguration.class);
    this.maxRetryCount = jwConf.getCommandMaxRetryCount();
    this.jobworkerNodeManager = ozoneManager.getJobworkerNodemanager();
    this.jobworkerCommandManager = ozoneManager.getOMJobworkerCommandManager();
  }

  @Override
  public void onSendCommand(OMJobworkerCommandProto command, UUID jobworkerUuid) {
    if (command.hasJobworkerMigrationKeysCommandProto()) {
      HddsProtos.JobworkerMigrationKeysCommandProto commandProto =
          command.getJobworkerMigrationKeysCommandProto();
      LOG.debug("Command {} Type {} key Count {} sent to JobWorker {}", commandProto.getCmdId(),
          command.getCommandType(), commandProto.getMigrationKeysTx().getMigrationKeysCount(),
          jobworkerUuid);
    } else {
      LOG.warn("Unrecognized command {}", command);
    }
  }

  @Override
  public void onCommandSucceeded(JobworkerCommandInfo statusInfo,
      CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
    LOG.debug("Key migration command {} succeeded on JobWorker {}",
        statusInfo.getCommandId(), jobworkerDetails.getUuidString());
    OMJobworkerMigrateKeyCommand command = (OMJobworkerMigrateKeyCommand) statusInfo.getCommand();
    String taskKey = command.getTaskKey();
    int keyCount = command.getMigrationKeysCount();
    long txId = command.getTxId();

    try {
      if (!taskManager.isTaskExists(taskKey)) {
        LOG.warn("Migration task {} not found in task table", taskKey);
        return;
      }
      if (!isValidMigrationResults(executionResultsProto, command, jobworkerDetails)) {
        taskManager.completeTransaction(taskKey, txId, keyCount);
        return;
      }

      MigrationResultsProto migrationResults = executionResultsProto.getMigrationResults();
      int successfulKeyCount = migrationResults.getSuccessfulKeyCount();
      LOG.debug("Key migration command {} succeeded on JobWorker {}, keyCount in command {}," +
              " migration results {}", statusInfo.getCommandId(), jobworkerDetails.getUuidString(),
          keyCount, migrationResults);
      if (successfulKeyCount == keyCount) {
        // All keys migrated successfully
        taskManager.completeTransaction(taskKey, txId, 0);
        return;
      }
      OMJobworkerMigrateKeyCommand retryCommand = handlePartialSuccessfulMigration(
          command, migrationResults, jobworkerDetails);
      if (retryCommand != null) {
        sendMigrationCommand(retryCommand);
      } else {
        taskManager.completeTransaction(taskKey, txId, keyCount - successfulKeyCount);
      }
    } catch (Exception e) {
      LOG.error("Error handling successful migration command {}",
          statusInfo.getCommandId(), e);
    }
  }

  @Override
  public void onCommandFailed(JobworkerCommandInfo statusInfo,
      CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
    LOG.warn("Key migration command {} failed on JobWorker {}, error code {} , error message {}",
        statusInfo.getCommandId(), jobworkerDetails.getUuidString(),
        statusInfo.getResultCode(), statusInfo.getMessage());

    try {
      OMJobworkerMigrateKeyCommand command = ((OMJobworkerMigrateKeyCommand) statusInfo.getCommand());
      String taskKey = command.getTaskKey();
      long txId = command.getTxId();
      if (!taskManager.isTaskExists(taskKey)) {
        LOG.warn("Migration task {} not found in task table", taskKey);
        return;
      }

      OMJobworkerMigrateKeyCommand retryCommand = handleFailedMigration(command,
          statusInfo.getResultCode(), jobworkerDetails);
      if (retryCommand != null) {
        sendMigrationCommand(retryCommand);
      } else {
        taskManager.completeTransaction(taskKey, txId, command.getMigrationKeysCount());
      }
    } catch (IOException e) {
      LOG.error("Error handling failed migration command {}",
          statusInfo.getCommandId(), e);
    }
  }

  @Override
  public void onStatusUpdateTimeout(JobworkerCommandInfo statusInfo, UUID jobworkerUuid) {
    LOG.warn("Key migration command {} timed out on JobWorker {}. " +
            "JobWorker may be unresponsive or command is stuck.",
        statusInfo.getCommandId(), jobworkerUuid);
    // The Jobworker may be offline, and all commands have not been updated,
    // resulting in a command update timeout.
    // To prevent infinite retries or the number of retries being used up quickly,
    // we will give up retrying the transaction.
    // The transaction is still in the DB, so it will be resent after a while.
  }

  @Override
  public void onCommandExecuting(JobworkerCommandInfo statusInfo, JobworkerDetails jobworkerDetails) {
    LOG.debug("Key migration command {} is executed on JobWorker {}",
        statusInfo.getCommandId(), jobworkerDetails.getUuidString());
  }

  private boolean isValidMigrationResults(CommandExecutionResultsProto executionResultsProto,
      OMJobworkerMigrateKeyCommand command, JobworkerDetails jobworkerDetails) {
    if (executionResultsProto == null || !executionResultsProto.hasMigrationResults()) {
      LOG.error("Command {} on JobWorker {} execution results are null",
          command.getId(), jobworkerDetails.getUuidString());
      return false;
    }
    MigrationResultsProto migrationResults = executionResultsProto.getMigrationResults();
    if (migrationResults.getResultsCount() != command.getMigrationKeysCount()) {
      LOG.error("Invalid command result, command key size{} != {} on JobWorker {}",
          command.getMigrationKeysCount(), migrationResults.getResultsCount(),
          jobworkerDetails.getUuidString());
      return false;
    }
    return true;
  }

  private OMJobworkerMigrateKeyCommand handlePartialSuccessfulMigration(
      OMJobworkerMigrateKeyCommand command, MigrationResultsProto migrationResults,
      JobworkerDetails jobworkerDetails) {
    int successCount = 0;
    int retryableCount = 0;
    int unrepeatableCount = 0;

    ArrayList<MigrationKeyProto> retryKeys = new ArrayList<>();
    int i = 0;
    for (MigrationKeyResult migrationKeyResult : migrationResults.getResultsList()) {
      migrationKeyResult.getKeyName();
      CommandResultCode resultCode = migrationKeyResult.getResultCode();
      if (resultCode == CommandResultCode.SUCCESS) {
        successCount++;
      } else {
        String failedKey = command.getMigrationKeys(i).getKey();
        if (!Objects.equals(failedKey, migrationKeyResult.getKeyName())) {
          LOG.warn("The key name in the command does not match the key name in the result {} !={}",
              failedKey, migrationKeyResult.getKeyName());
          return null;
        }
        if (shouldMakeTaskFail(resultCode)) {
          LOG.info("Migrating task {} failed due to {}", failedKey, resultCode);
          return null;
        }
        if (shouldRetry(resultCode, command, failedKey, jobworkerDetails)) {
          retryableCount++;
          retryKeys.add(
              MigrationKeyProto.newBuilder()
              .setKey(failedKey)
              .setUpdateID(command.getMigrationKeys(i).getUpdateID())
              .build()
          );
        } else {
          unrepeatableCount++;
        }
        LOG.debug("Key {} failed due to: {}", migrationKeyResult.getKeyName(), resultCode);
      }
      i++;
    }
    LOG.debug("handle partial Successful migration command: succeeded {}, retryable {}," +
        " unrepeatable {}", successCount, retryableCount, unrepeatableCount);
    if (!retryKeys.isEmpty()) {
      OMJobworkerMigrateKeyCommand retryCommand = new OMJobworkerMigrateKeyCommand(
          command.getTxId(),
          command.getVolume(),
          command.getBucket(),
          command.getReplicationConfig(),
          retryKeys,
          command.getPreserveAttributes(),
          command.getTaskKey(),
          command.getRetryCount() + 1);
      if (!exceedRetryCount(retryCommand)) {
        return retryCommand;
      }
    }

    return null;
  }

  private OMJobworkerMigrateKeyCommand handleFailedMigration(OMJobworkerMigrateKeyCommand command,
      CommandResultCode resultCode, JobworkerDetails jobworkerDetails) {
    if (shouldMakeTaskFail(resultCode)) {
      LOG.warn("Migration task {} will fail due to unrecoverable error: {} on the jobWorker {}",
          command.getTaskKey(), resultCode, jobworkerDetails.getUuidString());
      return null;
    }

    if (shouldRetry(resultCode, command, null, jobworkerDetails)) {
      OMJobworkerMigrateKeyCommand retryCommand = new OMJobworkerMigrateKeyCommand(
          command.getMigrationKeysTxProto(), command.getRetryCount() + 1);
      if (!exceedRetryCount(retryCommand)) {
        LOG.debug("Migration txProto {} will be retried", command.getTaskKey());
        return retryCommand;
      }
    }
    return null;
  }

  private void sendMigrationCommand(OMJobworkerMigrateKeyCommand command) throws IOException {
    UUID selectedJobworker = selectJobworkerForRetry();
    if (selectedJobworker != null) {
      jobworkerCommandManager.sendCommand(selectedJobworker, command);
      LOG.debug("Sent retry migration txProto for {} failed keys", command.getMigrationKeysCount());
    } else {
      // Do not retry command and do not update task status, this transaction will be sent again
      LOG.warn("No available JobWorker found for retry migration of");
    }
  }

  private UUID selectJobworkerForRetry() {
    List<JobworkerInfo> healthJobworkerInfos = jobworkerNodeManager
        .getNodeStateManager().getHealthyJobworkerInfos();
    if (healthJobworkerInfos.isEmpty()) {
      LOG.warn("No JobWorkers available health for retry migration");
      return null;
    }
    int randomIndex = ThreadLocalRandom.current().nextInt(healthJobworkerInfos.size());
    return healthJobworkerInfos.get(randomIndex).getUuid();
  }

  private boolean shouldMakeTaskFail(CommandResultCode resultCode) {
    switch (resultCode) {
    case VOLUME_NOT_FOUND:
    case BUCKET_NOT_FOUND:
      return true;
    default:
      return false;
    }
  }

  private boolean shouldRetry(CommandResultCode resultCode, OMJobworkerMigrateKeyCommand command,
      String keyName, JobworkerDetails jobworkerDetails) {
    switch (resultCode) {
    case UNKNOWN_CODE:
    case OTHER_ERROR:
    case PERMISSION_DENIED:
    case TYPE_MISMATCH:
    case COMMAND_EXPIRED:
    case STALE_TERM:
    case COMMAND_REJECTED:
    case UNEXPECTED_ERROR:
    case INVALID_COMMAND:
    case UNSUPPORTED_OM_SERVICE:
    case IO_TIMEOUT:
    case COMMAND_QUEUE_FULL:
      return true;
    case KEY_NOT_FOUND:
    case KEY_GENERATION_MISMATCH:
      return false; // Key has been deleted or rewritten, cannot retry.
    default:
      LOG.error("Unknown command result: {} for volume {}, bucket {}, key {} on the jobworker {}",
          resultCode, command.getVolume(), command.getBucket(), keyName, jobworkerDetails.getUuidString());
      return false;
    }
  }

  private boolean exceedRetryCount(OMJobworkerMigrateKeyCommand command) {
    return command.getRetryCount() > maxRetryCount;
  }

  @VisibleForTesting
  public JobworkerMigrationKeysTaskProto getTaskStatus(String taskKey) throws IOException {
    return taskManager.getTask(taskKey);
  }
}
