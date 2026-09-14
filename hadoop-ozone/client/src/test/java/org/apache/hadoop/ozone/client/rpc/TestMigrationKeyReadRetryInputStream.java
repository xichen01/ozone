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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.junit.jupiter.api.Test;

class TestMigrationKeyReadRetryInputStream {

  private static final byte[] DATA = "abcdef".getBytes(StandardCharsets.UTF_8);

  @Test
  void retriesReadAfterMigratedKeyRewrite() throws Exception {
    TestSeekableInputStream initial = new TestSeekableInputStream(DATA, 2);
    TestByteBufferInputStream reopened = new TestByteBufferInputStream(DATA, -1);
    AtomicInteger lookups = new AtomicInteger();
    MigrationKeyReadRetryInputStream stream = new MigrationKeyReadRetryInputStream(
        keyInfo(10, DATA.length, new HashMap<>()),
        new OzoneInputStream(initial),
        ignored -> {
          lookups.incrementAndGet();
          return keyInfo(11, DATA.length, migrationMetadata(10));
        },
        ignored -> new OzoneInputStream(reopened));

    byte[] prefix = new byte[2];
    assertEquals(2, stream.read(prefix, 0, prefix.length));
    assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), prefix);

    byte[] suffix = new byte[3];
    assertEquals(3, stream.read(suffix, 0, suffix.length));
    assertArrayEquals("cde".getBytes(StandardCharsets.UTF_8), suffix);
    assertEquals(2, reopened.getLastSeekPosition());
    assertEquals(5, stream.getPos());
    assertEquals(1, lookups.get());
  }

  @Test
  void resetsByteBufferPositionBeforeRetry() throws Exception {
    TestByteBufferInputStream initial = new TestByteBufferInputStream(DATA, 2);
    TestByteBufferInputStream reopened = new TestByteBufferInputStream(DATA, -1);
    MigrationKeyReadRetryInputStream stream = new MigrationKeyReadRetryInputStream(
        keyInfo(20, DATA.length, new HashMap<>()),
        new OzoneInputStream(initial),
        ignored -> keyInfo(21, DATA.length, migrationMetadata(20)),
        ignored -> new OzoneInputStream(reopened));

    ByteBuffer prefix = ByteBuffer.allocate(2);
    assertEquals(2, stream.read(prefix));
    ByteBuffer suffix = ByteBuffer.allocate(3);
    assertEquals(3, stream.read(suffix));
    assertArrayEquals("cde".getBytes(StandardCharsets.UTF_8), suffix.array());
    assertEquals(3, suffix.position());
  }

  @Test
  void doesNotRetryWhenRewriteMetadataDoesNotMatch() throws Exception {
    TestSeekableInputStream initial = new TestSeekableInputStream(DATA, 2);
    AtomicInteger lookups = new AtomicInteger();
    MigrationKeyReadRetryInputStream stream = new MigrationKeyReadRetryInputStream(
        keyInfo(30, DATA.length, new HashMap<>()),
        new OzoneInputStream(initial),
        ignored -> {
          lookups.incrementAndGet();
          return keyInfo(31, DATA.length, migrationMetadata(29));
        },
        ignored -> {
          throw new AssertionError("A non-migration rewrite must not reopen");
        });

    byte[] prefix = new byte[2];
    assertEquals(2, stream.read(prefix, 0, prefix.length));
    IOException failure = assertThrows(IOException.class,
        () -> stream.read(new byte[3], 0, 3));
    assertEquals("injected failure", failure.getMessage());
    assertEquals(1, lookups.get());
  }

  private static OmKeyInfo keyInfo(long generation, long dataSize,
      Map<String, String> metadata) {
    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setDataSize(dataSize)
        .setUpdateID(generation)
        .addAllMetadata(metadata)
        .build();
  }

  private static Map<String, String> migrationMetadata(long generation) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(OzoneConsts.REWRITE_SOURCE_VERSION, String.valueOf(generation));
    return metadata;
  }

  private static IOException injectedFailure() {
    return new StorageContainerException("injected failure",
        ContainerProtos.Result.CONTAINER_NOT_FOUND);
  }

  private static class TestInputStream extends InputStream {
    private final byte[] data;
    private final int failOnReadCall;
    private final IOException failure = injectedFailure();
    private int position;
    private int readCalls;

    TestInputStream(byte[] data, int failOnReadCall) {
      this.data = data;
      this.failOnReadCall = failOnReadCall;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      maybeFail();
      if (position >= data.length) {
        return -1;
      }
      int bytesRead = Math.min(length, data.length - position);
      System.arraycopy(data, position, buffer, offset, bytesRead);
      position += bytesRead;
      return bytesRead;
    }

    @Override
    public int read() throws IOException {
      byte[] buffer = new byte[1];
      return read(buffer, 0, 1) < 0 ? -1 : buffer[0] & 0xff;
    }

    private void maybeFail() throws IOException {
      readCalls++;
      if (readCalls == failOnReadCall) {
        throw failure;
      }
    }

    protected int getPosition() {
      return position;
    }

    protected void setPosition(int position) {
      this.position = position;
    }
  }

  private static class TestByteBufferInputStream extends TestInputStream
      implements ByteBufferReadable, Seekable {
    private long lastSeekPosition = -1;

    TestByteBufferInputStream(byte[] data, int failOnReadCall) {
      super(data, failOnReadCall);
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
      byte[] bytes = new byte[buffer.remaining()];
      int bytesRead = read(bytes, 0, bytes.length);
      if (bytesRead > 0) {
        buffer.put(bytes, 0, bytesRead);
      }
      return bytesRead;
    }

    @Override
    public void seek(long position) {
      lastSeekPosition = position;
      setPosition((int) position);
    }

    @Override
    public long getPos() {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long position) {
      seek(position);
      return false;
    }

    long getLastSeekPosition() {
      return lastSeekPosition;
    }
  }

  private static final class TestSeekableInputStream extends TestInputStream
      implements Seekable {
    private long lastSeekPosition = -1;

    TestSeekableInputStream(byte[] data, int failOnReadCall) {
      super(data, failOnReadCall);
    }

    @Override
    public void seek(long position) {
      this.lastSeekPosition = position;
      setPosition((int) position);
    }

    @Override
    public long getPos() {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long position) {
      seek(position);
      return false;
    }

    long getLastSeekPosition() {
      return lastSeekPosition;
    }
  }
}
