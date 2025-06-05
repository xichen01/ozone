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

import java.util.UUID;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command listener for key migration operations.
 * Handles the lifecycle events of key migration commands.
 */
public class MigrateKeyCommandListener implements JobworkerCommandListener {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateKeyCommandListener.class);

  // TODO Metrics successfulMigrations, failedMigrations, executingMigrations, retriedMigrations
  // TODO: Add reference to storage policy migration service when implemented

  /**
   * Constructor for MigrateKeyCommandListener.
   */
  public MigrateKeyCommandListener() {
  }

  @Override
  public void onSendCommand(OMJobworkerCommandProto command, UUID jobworkerUuid) {
    if (command.hasJobworkerMigrationKeysCommandProto()) {
      HddsProtos.JobworkerMigrationKeysCommandProto commandProto =
          command.getJobworkerMigrationKeysCommandProto();
      LOG.debug("Command {} Type {} key Count {} sent to JobWorker {}", commandProto.getCmdId(),
          command.getCommandType(), commandProto.getMigrationKeysTx().getMigrationKeys().size(),
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

    // TODO: Call business logic to complete the migration This should remove the
    //  migrated keys from the pending migration DB table
  }

  @Override
  public void onCommandFailed(JobworkerCommandInfo statusInfo,
      CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
  }

  @Override
  public void onCommandExecuting(JobworkerCommandInfo statusInfo, JobworkerDetails jobworkerDetails) {
    LOG.debug("Key migration command {} is executed on JobWorker {}",
        statusInfo.getCommandId(), jobworkerDetails.getUuidString());
  }

  @Override
  public void onStatusUpdateTimeout(JobworkerCommandInfo statusInfo, UUID jobworkerUuid) {
    LOG.warn("Key migration command {} timed out on JobWorker {}. " +
            "JobWorker may be unresponsive or command is stuck.",
        statusInfo.getCommandId(), jobworkerUuid);
  }

}
