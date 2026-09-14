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

import com.google.common.base.Preconditions;
import java.util.List;
import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;

/**
 * Command for migrating keys to the specified EC replication configuration.
 */
public class OMJobworkerMigrateKeyCommand extends OMJobworkerCommand<JobworkerMigrationKeysCommandProto>
    implements CopyObject<OMJobworkerMigrateKeyCommand> {

  private static final Codec<OMJobworkerMigrateKeyCommand> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(JobworkerMigrationKeysCommandProto.getDefaultInstance()),
      OMJobworkerMigrateKeyCommand::getFromProto,
      OMJobworkerMigrateKeyCommand::getProto,
      OMJobworkerMigrateKeyCommand.class);

  public static Codec<OMJobworkerMigrateKeyCommand> getCodec() {
    return CODEC;
  }

  private final JobworkerMigrationKeysTxProto migrationKeysTxProto;
  private final int retryCount;
  private final boolean verifyChecksum;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMJobworkerMigrateKeyCommand(long txId, String volume, String bucket,
      ECReplicationConfig replicationConfig,
      List<MigrationKeyProto> migrationKeys, String preserveAttributes, String taskKey,
      int retryCount) {
    this(txId, volume, bucket, replicationConfig, migrationKeys, preserveAttributes,
        taskKey, retryCount, false);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public OMJobworkerMigrateKeyCommand(long txId, String volume, String bucket,
      ECReplicationConfig replicationConfig,
      List<MigrationKeyProto> migrationKeys, String preserveAttributes, String taskKey,
      int retryCount, boolean verifyChecksum) {
    Preconditions.checkNotNull(volume, "Volume cannot be null");
    Preconditions.checkNotNull(bucket, "Bucket cannot be null");
    Preconditions.checkNotNull(replicationConfig, "Replication config cannot be null");
    Preconditions.checkNotNull(migrationKeys, "migrationKeys cannot be null");
    Preconditions.checkNotNull(taskKey, "Task status key cannot be null");
    JobworkerMigrationKeysTxProto.Builder builder =
        JobworkerMigrationKeysTxProto.newBuilder()
            .setTxId(txId)
            .setVolume(volume)
            .setBucket(bucket)
            .setEcReplicationConfig(replicationConfig.toProto())
            .addAllMigrationKeys(migrationKeys)
            .setTaskKey(taskKey);
    if (preserveAttributes != null) {
      builder.setPreserveAttributes(preserveAttributes);
    }
    migrationKeysTxProto = builder.build();
    this.retryCount = retryCount;
    this.verifyChecksum = verifyChecksum;
  }

  /**
   * Full constructor for JobworkerMigrateKeyCommand with tags support.
   *
   * @param retryCount The retry count for this command
   */
  public OMJobworkerMigrateKeyCommand(JobworkerMigrationKeysTxProto migrationKeysTxProto,
      int retryCount) {
    Preconditions.checkNotNull(migrationKeysTxProto);
    this.migrationKeysTxProto = migrationKeysTxProto;
    this.retryCount = retryCount;
    this.verifyChecksum = false;
  }

  public OMJobworkerMigrateKeyCommand(JobworkerMigrationKeysTxProto migrationKeysTxProto,
      int retryCount, boolean verifyChecksum) {
    Preconditions.checkNotNull(migrationKeysTxProto);
    this.migrationKeysTxProto = migrationKeysTxProto;
    this.retryCount = retryCount;
    this.verifyChecksum = verifyChecksum;
  }

  /**
   * Full constructor for JobworkerMigrateKeyCommand with tags support.
   *
   * @param retryCount The retry count for this command
   */
  private OMJobworkerMigrateKeyCommand(JobworkerMigrationKeysTxProto migrationKeysTxProto, long cmdId,
      int retryCount, boolean verifyChecksum) {
    super(cmdId);
    Preconditions.checkNotNull(migrationKeysTxProto);
    this.migrationKeysTxProto = migrationKeysTxProto;
    this.retryCount = retryCount;
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public OMJobworkerCommandProto.Type getType() {
    return OMJobworkerCommandProto.Type.migrateKeyCommand;
  }

  @Override
  public JobworkerMigrationKeysCommandProto getProto() {
    JobworkerMigrationKeysCommandProto.Builder builder = JobworkerMigrationKeysCommandProto.newBuilder()
        .setCmdId(getId())
        .setMigrationKeysTx(migrationKeysTxProto)
        .setRetryCount(retryCount)
        .setVerifyChecksum(verifyChecksum);
    return builder.build();
  }

  public static OMJobworkerMigrateKeyCommand getFromProto(JobworkerMigrationKeysCommandProto proto) {
    return new OMJobworkerMigrateKeyCommand(
        proto.getMigrationKeysTx(), proto.getCmdId(), proto.getRetryCount(),
        proto.getVerifyChecksum());
  }

  public String getVolume() {
    return migrationKeysTxProto.getVolume();
  }

  public long getTxId() {
    return migrationKeysTxProto.getTxId();
  }

  public String getBucket() {
    return migrationKeysTxProto.getBucket();
  }

  public ECReplicationConfig getReplicationConfig() {
    return new ECReplicationConfig(migrationKeysTxProto.getEcReplicationConfig());
  }

  public List<MigrationKeyProto> getMigrationKeys() {
    return migrationKeysTxProto.getMigrationKeysList();
  }

  public int getMigrationKeysCount() {
    return migrationKeysTxProto.getMigrationKeysCount();
  }

  public MigrationKeyProto getMigrationKeys(int index) {
    return migrationKeysTxProto.getMigrationKeys(index);
  }

  @Nullable
  public String getPreserveAttributes() {
    return migrationKeysTxProto.getPreserveAttributes();
  }

  public int getRetryCount() {
    return retryCount;
  }

  public boolean isVerifyChecksum() {
    return verifyChecksum;
  }

  public String getTaskKey() {
    return migrationKeysTxProto.getTaskKey();
  }

  public JobworkerMigrationKeysTxProto getMigrationKeysTxProto() {
    return migrationKeysTxProto;
  }

  @Override
  public String toString() {
    return "JobworkerMigrateKeyCommand{" +
        "volume='" + migrationKeysTxProto.getVolume() + '\'' +
        ", bucket='" + migrationKeysTxProto.getBucket() + '\'' +
        ", replicationConfig=" + migrationKeysTxProto.getEcReplicationConfig() +
        ", keyCount=" + migrationKeysTxProto.getMigrationKeysCount() +
        ", preserveAttributes=" + migrationKeysTxProto.getPreserveAttributes() +
        ", taskKey=" + migrationKeysTxProto.getTaskKey() +
        ", retryCount=" + retryCount +
        '}';
  }

  @Override
  public OMJobworkerMigrateKeyCommand copyObject() {
    return new OMJobworkerMigrateKeyCommand(getMigrationKeysTxProto(),
        getRetryCount(), isVerifyChecksum());
  }

}
