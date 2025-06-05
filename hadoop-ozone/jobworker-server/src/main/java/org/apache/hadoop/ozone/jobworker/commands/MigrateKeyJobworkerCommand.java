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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysCommandProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerMigrationKeysTxProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.MigrationKeyProto;

/**
 * JobWorker side command for key migration.
 */
public final class MigrateKeyJobworkerCommand extends JobworkerCommand<JobworkerMigrationKeysCommandProto> {

  private final JobworkerMigrationKeysTxProto txProto;

  private final AtomicInteger retryCount;

  private MigrateKeyJobworkerCommand(long id, String omServiceId, long term,
      long expirationTimestampMs, JobworkerMigrationKeysTxProto txProto, int retryCount) {
    super(id, omServiceId, term, expirationTimestampMs);
    this.txProto = txProto;
    this.retryCount = new AtomicInteger(retryCount);
  }

  /**
   * Factory method to create command from OM command proto.
   */
  public static MigrateKeyJobworkerCommand fromOMCommand(
      OMJobworkerCommandProto omCommandProto, String omServiceId, long term) {

    if (!omCommandProto.hasJobworkerMigrationKeysCommandProto()) {
      throw new IllegalArgumentException("Missing migration command proto");
    }

    JobworkerMigrationKeysCommandProto migrationProto =
        omCommandProto.getJobworkerMigrationKeysCommandProto();

    long expiration = omCommandProto.hasExpirationTimestampMs() ?
        omCommandProto.getExpirationTimestampMs() : 0;

    return new MigrateKeyJobworkerCommand(migrationProto.getCmdId(), omServiceId, term, expiration,
        migrationProto.getMigrationKeysTx(), migrationProto.getRetryCount());
  }

  @Override
  public OMJobworkerCommandProto.Type getType() {
    return OMJobworkerCommandProto.Type.migrateKeyCommand;
  }

  @Override
  public JobworkerMigrationKeysCommandProto getProto() {
    return JobworkerMigrationKeysCommandProto.newBuilder()
        .setCmdId(getId())
        .setRetryCount(retryCount.get())
        .setMigrationKeysTx(txProto)
        .build();
  }

  /**
   * Increment retry count and return new count.
   */
  public int incrementRetryCount() {
    return retryCount.incrementAndGet();
  }


  public long getTxId() {
    return txProto.getTxId();
  }

  public String getVolume() {
    return txProto.getVolume();
  }

  public String getBucket() {
    return txProto.getBucket();
  }

  public ECReplicationConfig getReplicationConfig() {
    return ECReplicationConfig.fromProto(txProto.getEcReplicationConfig());
  }

  public List<MigrationKeyProto> getMigrationKeys() {
    return txProto.getMigrationKeysList();
  }

  /**
   * Get current retry count.
   */
  public int getRetryCount() {
    return retryCount.get();
  }

  @Override
  public String toString() {
    return "MigrateKeyJobworkerCommand{" +
        "id=" + getId() +
        ", txId='" + txProto.getTxId() + '\'' +
        ", volume='" + txProto.getVolume() + '\'' +
        ", bucket='" + txProto.getBucket() + '\'' +
        ", replicationConfig=" + txProto.getEcReplicationConfig() +
        ", keyCount=" + txProto.getMigrationKeysCount() +
        ", preserveAttributes=" + txProto.getPreserveAttributes() +
        ", currentRetryCount=" + retryCount.get() +
        ", omServiceId='" + getOmServiceId() + '\'' +
        '}';
  }

  @Nullable
  public String getPreserveAttributes() {
    return txProto.getPreserveAttributes();
  }
}
