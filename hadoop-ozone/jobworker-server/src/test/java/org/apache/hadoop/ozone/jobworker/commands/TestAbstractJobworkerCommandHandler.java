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
package org.apache.hadoop.ozone.jobworker.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/**
 * Tests for AbstractJobworkerCommandHandler class.
 */
public class TestAbstractJobworkerCommandHandler {

  private JobworkerStateContext context;
  private JobworkerConnectionManager connectionManager;
  private MockCommandHandler handler;
  private JobworkerCommandManager commandManager;

  @BeforeEach
  public void setUp() {
    context = mock(JobworkerStateContext.class);
    connectionManager = mock(JobworkerConnectionManager.class);
    commandManager = mock(JobworkerCommandManager.class);
    handler = new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand);
  }

  @Test
  public void testBasicHandling() throws ExecutionException, InterruptedException, TimeoutException {
    handler.setProcessDelayMs(2);
    handler.enablePause();
    JobworkerCommand<?> command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);

    CompletableFuture<?> future = CompletableFuture.runAsync(() -> {
      handler.handle(command, context, connectionManager);
    });
    GenericTestUtils.waitFor(() -> handler.getQueuedCount() == 1, 50, 1000);
    handler.releasePause();
    future.get();
    assertEquals(0, handler.getQueuedCount());
    assertTrue(handler.wasProcessCommandCalled());
    assertEquals(1, handler.getInvocationCount());
    assertTrue(handler.getTotalRunTime() > 0);
  }

  @Test
  public void testExpiredCommand() {
    // Create a command with an expired deadline
    MockJobworkerCommand command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);
    command.setDeadline(System.currentTimeMillis() - 1000); // Expired
    JobworkerCommandStatus mockStatus = mock(JobworkerCommandStatus.class);
    when(context.getCommandManager()).thenReturn(commandManager);
    when(commandManager.getCmdStatus(anyLong())).thenReturn(mockStatus);

    handler.handle(command, context, connectionManager);
    assertFalse(handler.wasProcessCommandCalled());

    verify(mockStatus).setStatus(eq(CommandStatus.Status.FAILED));
    verify(mockStatus).setMessage(any(String.class));
  }

  @Test
  public void testExceptionHandling() {
    JobworkerCommand<?> command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);
    Exception testException = new RuntimeException("Test exception");
    handler.setExceptionToThrow(testException);
    JobworkerCommandStatus mockStatus = mock(JobworkerCommandStatus.class);
    when(context.getCommandManager()).thenReturn(commandManager);
    when(commandManager.getCmdStatus(anyLong())).thenReturn(mockStatus);

    handler.handle(command, context, connectionManager);

    assertTrue(handler.wasProcessCommandCalled());
    verify(mockStatus).setStatus(eq(CommandStatus.Status.FAILED));
    verify(mockStatus).setMessage(any(String.class));
    assertEquals(1, handler.getInvocationCount());
  }

  @Test
  public void testUpdateCommandStatus() {
    JobworkerCommandStatus mockStatus = mock(JobworkerCommandStatus.class);
    JobworkerCommand<?> command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.mockCommand);
    when(context.getCommandManager()).thenReturn(commandManager);
    when(commandManager.getCmdStatus(eq(1L))).thenReturn(mockStatus);
    Consumer<JobworkerCommandStatus> statusUpdater = status -> {
      status.setStatus(CommandStatus.Status.SUCCEEDED);
      status.setMessage("Success");
    };

    handler.updateCommandStatus(context, command, statusUpdater, mock(Logger.class));
    // Command should be updated normally
    verify(mockStatus).setStatus(CommandStatus.Status.SUCCEEDED);
    verify(mockStatus).setMessage("Success");

  }
}
