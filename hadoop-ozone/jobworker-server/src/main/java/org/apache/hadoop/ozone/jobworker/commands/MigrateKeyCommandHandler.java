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

package org.apache.hadoop.ozone.jobworker.commands;

import static org.apache.hadoop.ozone.OzoneConsts.OM_SERVICE_ID_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ObjectAttributes;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationKeyResult;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MigrationResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.conf.JobWorkerMigrationKeyConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for key migration commands.
 * Responsible for copying keys with a new EC replication configuration.
 */
public class MigrateKeyCommandHandler extends AbstractJobworkerCommandHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateKeyCommandHandler.class);

  private final int bufferSize;
  private final ConfigurationSource conf;
  private final Collection<String> serviceIdList;

  /**
   * Constructor for MigrateKeyCommandHandler.
   *
   * @param executorService Thread pool for command execution
   * @param conf            Ozone configuration
   */
  public MigrateKeyCommandHandler(ExecutorService executorService, ConfigurationSource conf) {
    super(OMJobworkerCommandProto.Type.migrateKeyCommand, executorService);
    this.conf = conf;
    JobWorkerMigrationKeyConfiguration jwConf = conf.getObject(JobWorkerMigrationKeyConfiguration.class);
    int size = jwConf.getBufferSize();
    if (size <= 0) {
      LOG.warn("Buffer size is set to default: {}", IOUtils.DEFAULT_BUFFER_SIZE);
      size = IOUtils.DEFAULT_BUFFER_SIZE;
    }
    this.bufferSize = size;
    this.serviceIdList = conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
  }

  /**
   * Create MigrateKeyCommandHandler with configuration-based thread pool.
   *
   * @param threadNamePrefix Thread name prefix
   * @param conf Ozone configuration
   * @return MigrateKeyCommandHandler instance
   */
  public static MigrateKeyCommandHandler create(String threadNamePrefix, ConfigurationSource conf) {
    JobWorkerMigrationKeyConfiguration jwConf = conf.getObject(JobWorkerMigrationKeyConfiguration.class);
    int threadPoolSize = jwConf.getThreadPoolSize();

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat(threadNamePrefix + "MigrateKeyHandler-%d")
        .setDaemon(true)
        .build();
    ExecutorService executorService = new ThreadPoolExecutor(
        0, threadPoolSize, 10, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(), threadFactory, new ThreadPoolExecutor.AbortPolicy());
    return new MigrateKeyCommandHandler(executorService, conf);
  }

  @Override
  protected boolean processCommand(JobworkerCommand<?> command,
                                   JobworkerStateContext context,
                                   JobworkerConnectionManager connectionManager) throws Exception {
    if (!validateCommandType(command, context)) {
      return false;
    }
    MigrateKeyJobworkerCommand migrateCommand = (MigrateKeyJobworkerCommand) command;
    return executeMigration(migrateCommand, context);
  }

  private boolean validateCommandType(JobworkerCommand<?> command, JobworkerStateContext context) {
    if (!(command instanceof MigrateKeyJobworkerCommand)) {
      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
          "Unexpected command type", CommandResultCode.INVALID_COMMAND);
      LOG.error("Expected MigrateKeyJobworkerCommand, got {}", command.getClass().getSimpleName());
      return false;
    }
    return true;
  }

  private boolean executeMigration(MigrateKeyJobworkerCommand command, JobworkerStateContext context) {
    long startTime = Time.monotonicNow();
    String volumeName = command.getVolume();
    String bucketName = command.getBucket();
    ECReplicationConfig replicationConfig = command.getReplicationConfig();
    LOG.info("Processing key migration: " +
            "OmServiceId={}, volume={}, bucket={}, replicationConfig={}, keyCount={}, preserveAttributes={}, commandId={} ",
        command.getOmServiceId(), volumeName, bucketName, replicationConfig, command.getMigrationKeys().size(),
        command.getPreserveAttributes(), command.getId());
    if (command.getMigrationKeys().isEmpty()) {
      return true;
    }

    // TODO can make a single JW to have one OzoneClient per OM service, instead of creating one for each command.
    try (OzoneClient ozoneClient = createOzoneClient(command, context)) {
      if (ozoneClient == null) {
        return false;
      }
      OzoneBucket bucket = getBucket(ozoneClient, command, context);
      if (bucket == null) {
        return false;
      }

      int successfulKeys = migrateKeys(bucket, command, context, replicationConfig);
      LOG.info("Key migration completed: " +
              "OmServiceId={}, volume={}, bucket={}, replicationConfig={}, keyCount={}, preserveAttributes={}" +
              ", commandId={}, duration={}ms",
          command.getOmServiceId(), volumeName, bucketName, replicationConfig, command.getMigrationKeys().size(),
          command.getPreserveAttributes(), command.getId(), Time.monotonicNow() - startTime);

      return successfulKeys > 0;
    } catch (Exception e) {
      LOG.error("Error during key migration", e);
      return false;
    }
  }

  private OzoneClient createOzoneClient(MigrateKeyJobworkerCommand command, JobworkerStateContext context) {
    String omServiceId = command.getOmServiceId();

    try {
      if (serviceIdList.isEmpty()) {
        if (omServiceId.equals(OM_SERVICE_ID_DEFAULT)) {
          return OzoneClientFactory.getRpcClient(conf);
        }
      } else if (serviceIdList.contains(omServiceId)) {
        return OzoneClientFactory.getRpcClient(omServiceId, conf);
      }

      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
          "Unsupported OM Service: " + omServiceId, CommandResultCode.UNSUPPORTED_OM_SERVICE);
      LOG.error("Cannot get OzoneClient for {}. Supported services: {}", omServiceId, serviceIdList);
      return null;
    } catch (IOException e) {
      updateCommandStatus(context, command, CommandStatus.Status.FAILED,
          e.getMessage(), CommandResultCode.CLIENT_CREATION_FAILED);
      LOG.error("Failed to create OzoneClient", e);
      return null;
    }
  }

  private OzoneBucket getBucket(OzoneClient ozoneClient, MigrateKeyJobworkerCommand command,
                                JobworkerStateContext context) {
    try {
      return ozoneClient.getObjectStore()
          .getVolume(command.getVolume())
          .getBucket(command.getBucket());
    } catch (IOException e) {
      CommandResultCode resultCode = getCommandResultCode(e);
      if (resultCode != null) {
        updateCommandStatus(context, command, CommandStatus.Status.FAILED, e.getMessage(), resultCode);
      }
      LOG.error("Failed to get bucket: {}/{}", command.getVolume(), command.getBucket(), e);
      return null;
    }
  }

  private int migrateKeys(OzoneBucket bucket, MigrateKeyJobworkerCommand command,
                          JobworkerStateContext context, ECReplicationConfig replicationConfig) {
    int successfulKeys = 0;
    MigrationResultsProto.Builder migrationResults = MigrationResultsProto.newBuilder();

    try {
      for (MigrationKeyProto migrationKeyProto : command.getMigrationKeys()) {
        MigrationKeyResult.Builder keyResult = MigrationKeyResult.newBuilder().setKeyName(migrationKeyProto.getKey());

        try {
          migrateKey(bucket, migrationKeyProto, command.getPreserveAttributes(), replicationConfig);
          keyResult.setResultCode(CommandResultCode.SUCCESS);
          migrationResults.addResults(keyResult.build());
          successfulKeys++;
          LOG.debug("Successfully migrated key: {} to replication config {}",
              migrationKeyProto.getKey(), replicationConfig);
        } catch (IOException e) {
          CommandResultCode resultCode = getCommandResultCode(e);
          keyResult.setResultCode(resultCode != null ? resultCode : CommandResultCode.OTHER_ERROR);
          migrationResults.addResults(keyResult.build());
          LOG.warn("Failed to migrate key: {}", migrationKeyProto.getKey(), e);
        }
      }
    } finally {
      setExecutionResults(context, command,
          CommandExecutionResultsProto.newBuilder()
              .setMigrationResults(migrationResults.setSuccessfulKeyCount(successfulKeys).build())
              .build());
    }

    return successfulKeys;
  }

  @Nullable
  private static CommandResultCode getCommandResultCode(IOException e) {
    if (e == null) {
      return null;
    }
    if (e instanceof OMException) {
      OMException omException = (OMException) e;
      ResultCodes result = omException.getResult();
      switch (result) {
      case KEY_NOT_FOUND:
        if (omException.getMessage().contains("Generation mismatch")) {
          return CommandResultCode.KEY_GENERATION_MISMATCH;
        }
        return CommandResultCode.KEY_NOT_FOUND;
      case BUCKET_NOT_FOUND:
        return CommandResultCode.BUCKET_NOT_FOUND;
      case VOLUME_NOT_FOUND:
        return CommandResultCode.VOLUME_NOT_FOUND;
      case PERMISSION_DENIED:
        return CommandResultCode.PERMISSION_DENIED;
      default:
        return null;
      }
    }
    if (e.getCause() instanceof TimeoutException) {
      return CommandResultCode.IO_TIMEOUT;
    }
    if (e.getMessage().contains("Error parsing preserve attributes")) {
      return CommandResultCode.INVALID_COMMAND;
    }

    return null;
  }

  private void migrateKey(OzoneBucket bucket, MigrationKeyProto migrationKeyProto, String preserveAttributesStr,
      ECReplicationConfig targetReplicationConfig) throws IOException {
    OzoneKeyDetails sourceKeyDetails = bucket.getKey(migrationKeyProto.getKey());
    // Skip if already has the target replication configuration.
    if (sourceKeyDetails.getReplicationConfig().equals(targetReplicationConfig)) {
      LOG.debug("Key {} already has replication config {}", migrationKeyProto, targetReplicationConfig);
      return;
    }
    ObjectAttributes objectAttributes = parsePreserveAttributes(sourceKeyDetails, preserveAttributesStr);
    long sourceKeyLen = sourceKeyDetails.getDataSize();
    try (OzoneInputStream inputStream = sourceKeyDetails.getContent();
         OzoneOutputStream outputStream = bucket.rewriteKey(
             migrationKeyProto.getKey(), sourceKeyLen, migrationKeyProto.getUpdateID(),
             targetReplicationConfig, sourceKeyDetails.getMetadata(),
             sourceKeyDetails.getTags(), objectAttributes)) {

      IOUtils.copyLarge(inputStream, outputStream, 0, sourceKeyLen,
          new byte[getIOBufferSize(sourceKeyLen)]);
    }
  }

  private int getIOBufferSize(long fileLength) {
    return fileLength == 0 ? bufferSize :
           (fileLength < bufferSize ? (int) fileLength : bufferSize);
  }

  private static ObjectAttributes parsePreserveAttributes(OzoneKeyDetails ozoneKeyDetails,
      String preserveAttributesStr) throws IOException {
    try {
      if (StringUtils.isEmpty(preserveAttributesStr)) {
        return null;
      }
      ArrayList<ObjectAttributes.AttributeType> attributes;
      attributes = new ArrayList<>();
      for (char preserveSymbol : preserveAttributesStr.toCharArray()) {
        ObjectAttributes.AttributeType attributeType = ObjectAttributes.AttributeType.fromSymbol(preserveSymbol);
        attributes.add(attributeType);
      }

      ObjectAttributes objectAttributes = new ObjectAttributes();
      for (ObjectAttributes.AttributeType preserveAttribute : attributes) {
        switch (preserveAttribute) {
        case USER: // Preserve username
          if (StringUtils.isNotBlank(ozoneKeyDetails.getOwnerName())) {
            objectAttributes.setUsername(ozoneKeyDetails.getOwnerName());
          }
          break;
        case MTIME: // Preserve modification time
          long mtime = ozoneKeyDetails.getModificationTime().toEpochMilli();
          if (mtime > 0L) {
            objectAttributes.setMtime(mtime);
          }
          break;
        case CTIME: // Preserve modification time
          long ctime = ozoneKeyDetails.getCreationTime().toEpochMilli();
          if (ctime > 0L) {
            objectAttributes.setCtime(ctime);
          }
          break;
        default:
          throw new IllegalArgumentException("Invalid preserve attribute: " + preserveAttribute);
        }
      }
      return objectAttributes;
    } catch (Throwable e) {
      LOG.error("Error parsing preserve attributes", e);
      throw new IOException("Error parsing preserve attributes");
    }
  }
}
