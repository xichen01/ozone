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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps an {@link OzoneInputStream} and retries key reads after a migration
 * changes the key's block layout.
 */
final class MigrationKeyReadRetryInputStream extends InputStream
    implements CanUnbuffer, ByteBufferReadable, Seekable {

  private static final Logger LOG =
      LoggerFactory.getLogger(MigrationKeyReadRetryInputStream.class);
  private static final int REPLAY_BUFFER_SIZE = 8192;
  private static final Runnable NO_OP = () -> { };

  private final Function<OmKeyInfo, OmKeyInfo> keyInfoLookup;
  private final CheckedFunction<OmKeyInfo, OzoneInputStream, IOException>
      streamFactory;

  private OmKeyInfo currentKeyInfo;
  private OzoneInputStream currentStream;
  private long position;
  private boolean closed;

  MigrationKeyReadRetryInputStream(OmKeyInfo keyInfo,
      OzoneInputStream inputStream,
      Function<OmKeyInfo, OmKeyInfo> keyInfoLookup,
      CheckedFunction<OmKeyInfo, OzoneInputStream, IOException> streamFactory) {
    this.currentKeyInfo = keyInfo;
    this.currentStream = inputStream;
    this.keyInfoLookup = keyInfoLookup;
    this.streamFactory = streamFactory;
  }

  @Override
  public synchronized int read() throws IOException {
    checkOpen();
    int value = withMigrationRetry(position, currentStream::read);
    if (value >= 0) {
      position++;
    }
    return value;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len)
      throws IOException {
    checkOpen();
    if (len == 0) {
      return 0;
    }

    int bytesRead = withMigrationRetry(position,
        () -> currentStream.read(b, off, len));
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public synchronized int read(ByteBuffer byteBuffer) throws IOException {
    checkOpen();
    int originalPosition = byteBuffer.position();
    int bytesRead = withMigrationRetry(position,
        () -> byteBuffer.position(originalPosition),
        () -> currentStream.read(byteBuffer));
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    checkOpen();
    if (n <= 0) {
      return 0;
    }

    long skipped = withMigrationRetry(position, () -> currentStream.skip(n));
    if (skipped > 0) {
      position += skipped;
    }
    return skipped;
  }

  @Override
  public synchronized int available() throws IOException {
    checkOpen();
    return currentStream.available();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    currentStream.close();
  }

  @Override
  public synchronized void unbuffer() {
    currentStream.unbuffer();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkOpen();
    currentStream.seek(pos);
    position = pos;
  }

  @Override
  public synchronized long getPos() {
    return position;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    checkOpen();
    boolean seeked = currentStream.seekToNewSource(targetPos);
    position = targetPos;
    return seeked;
  }

  private <T> T withMigrationRetry(long readStartPosition,
      Runnable beforeRetry, CheckedSupplier<T, IOException> operation)
      throws IOException {
    try {
      return operation.get();
    } catch (IOException failure) {
      if (!reopenMigratedKeyStream(readStartPosition, failure)) {
        throw failure;
      }
      beforeRetry.run();
      return operation.get();
    }
  }

  private <T> T withMigrationRetry(long readStartPosition,
      CheckedSupplier<T, IOException> operation) throws IOException {
    return withMigrationRetry(readStartPosition, NO_OP, operation);
  }

  private boolean reopenMigratedKeyStream(long readStartPosition,
      IOException failure) throws IOException {
    OmKeyInfo latestKeyInfo = keyInfoLookup.apply(currentKeyInfo);
    if (!isVersionCompatibleRewriteOfCurrentKey(latestKeyInfo)) {
      return false;
    }

    OzoneInputStream reopenedStream = null;
    long previousGeneration = failureSafeGeneration(currentKeyInfo);
    try {
      reopenedStream = streamFactory.apply(latestKeyInfo);
      repositionStream(reopenedStream, readStartPosition);
      closeCurrentStreamQuietly();
      currentStream = reopenedStream;
      currentKeyInfo = latestKeyInfo;
      position = readStartPosition;
      LOG.info("Retrying key read for migrated key {}/{}/{} from offset {} "
              + "after generation {} switched to {}.",
          latestKeyInfo.getVolumeName(), latestKeyInfo.getBucketName(),
          latestKeyInfo.getKeyName(), readStartPosition, previousGeneration,
          failureSafeGeneration(latestKeyInfo));
      return true;
    } catch (IOException reopenFailure) {
      if (reopenedStream != null) {
        try {
          reopenedStream.close();
        } catch (IOException closeFailure) {
          reopenFailure.addSuppressed(closeFailure);
        }
      }
      failure.addSuppressed(reopenFailure);
      LOG.debug("Unable to reopen migrated key {}/{}/{} from offset {}.",
          currentKeyInfo.getVolumeName(), currentKeyInfo.getBucketName(),
          currentKeyInfo.getKeyName(), readStartPosition, reopenFailure);
      return false;
    }
  }

  private boolean isVersionCompatibleRewriteOfCurrentKey(
      OmKeyInfo latestKeyInfo) {
    if (latestKeyInfo == null
        || latestKeyInfo.getGeneration() == currentKeyInfo.getGeneration()) {
      return false;
    }

    Map<String, String> metadata = latestKeyInfo.getMetadata();
    String sourceVersion = metadata.get(OzoneConsts.REWRITE_SOURCE_VERSION);
    if (sourceVersion == null) {
      return false;
    }

    try {
      if (Long.parseLong(sourceVersion) != currentKeyInfo.getGeneration()) {
        return false;
      }
      if (latestKeyInfo.getDataSize() != currentKeyInfo.getDataSize()) {
        LOG.warn("Skip migration read retry for key {}/{}/{} because data "
                + "size changed from {} to {}.",
            latestKeyInfo.getVolumeName(), latestKeyInfo.getBucketName(),
            latestKeyInfo.getKeyName(), currentKeyInfo.getDataSize(),
            latestKeyInfo.getDataSize());
        return false;
      }
      return true;
    } catch (NumberFormatException e) {
      LOG.warn("Invalid rewrite source version {} on key {}/{}/{}.",
          sourceVersion, latestKeyInfo.getVolumeName(),
          latestKeyInfo.getBucketName(), latestKeyInfo.getKeyName(), e);
      return false;
    }
  }

  private void repositionStream(OzoneInputStream reopenedStream,
      long readStartPosition) throws IOException {
    if (readStartPosition == 0) {
      return;
    }

    try {
      reopenedStream.seek(readStartPosition);
    } catch (UnsupportedOperationException seekFailure) {
      LOG.debug("Reopened key stream for {}/{}/{} does not support seeking "
              + "to offset {}. Replaying from the beginning instead.",
          currentKeyInfo.getVolumeName(), currentKeyInfo.getBucketName(),
          currentKeyInfo.getKeyName(), readStartPosition, seekFailure);
      replayFromStart(reopenedStream, readStartPosition);
    }
  }

  private void replayFromStart(OzoneInputStream reopenedStream,
      long targetPosition) throws IOException {
    byte[] buffer = new byte[REPLAY_BUFFER_SIZE];
    long remaining = targetPosition;
    while (remaining > 0) {
      int bytesRead = reopenedStream.read(buffer, 0,
          (int) Math.min(buffer.length, remaining));
      if (bytesRead < 0) {
        throw new EOFException("Unable to replay migrated key stream to offset "
            + targetPosition + ".");
      }
      remaining -= bytesRead;
    }
  }

  private void closeCurrentStreamQuietly() {
    try {
      currentStream.close();
    } catch (IOException closeFailure) {
      LOG.debug("Unable to close key stream for {}/{}/{} while switching to "
              + "a migrated stream.", currentKeyInfo.getVolumeName(),
          currentKeyInfo.getBucketName(), currentKeyInfo.getKeyName(),
          closeFailure);
    }
  }

  private static long failureSafeGeneration(OmKeyInfo keyInfo) {
    return keyInfo != null ? keyInfo.getGeneration() : -1L;
  }

  private void checkOpen() throws IOException {
    if (closed) {
      throw new IOException("The stream is closed");
    }
  }
}
