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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for JobworkerCommandDispatcher class.
 */
public class TestJobworkerCommandDispatcher {

  private JobworkerStateContext context;
  private JobworkerConnectionManager connectionManager;
  private JobworkerCommandDispatcher dispatcher;
  private MockCommandHandler reRegisterHandler;
  private MockCommandHandler mockCommandHandler;

  @BeforeEach
  public void setUp() {
    context = mock(JobworkerStateContext.class);
    connectionManager = mock(JobworkerConnectionManager.class);

    // Create handlers for different command types
    reRegisterHandler = new MockCommandHandler(OMJobworkerCommandProto.Type.reregisterCommand);
    mockCommandHandler = new MockCommandHandler(OMJobworkerCommandProto.Type.mockCommand);

    dispatcher = spy(JobworkerCommandDispatcher.newBuilder()
        .addHandler(reRegisterHandler)
        .addHandler(mockCommandHandler)
        .setConnectionManager(connectionManager)
        .setContext(context)
        .build());
  }

  @Test
  public void testHandlerRegistration() {
    JobworkerCommandHandler retrievedHandler1 =
        dispatcher.getHandler(OMJobworkerCommandProto.Type.reregisterCommand);
    JobworkerCommandHandler retrievedHandler2 =
        dispatcher.getHandler(OMJobworkerCommandProto.Type.mockCommand);

    assertNotNull(retrievedHandler1);
    assertNotNull(retrievedHandler2);
    assertEquals(reRegisterHandler, retrievedHandler1);
    assertEquals(mockCommandHandler, retrievedHandler2);
  }

  @Test
  public void testCommandDispatching() {
    JobworkerCommand<?> command1 =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand);
    JobworkerCommand<?> command2 =
        new MockJobworkerCommand(2L, OMJobworkerCommandProto.Type.mockCommand);
    // Did not add Handler for the unknownCommand command
    JobworkerCommand<?> command3 =
        new MockJobworkerCommand(3L, OMJobworkerCommandProto.Type.unknownCommand);

    dispatcher.handle(command1);
    dispatcher.handle(command2);
    dispatcher.handle(command3); // Should log but not throw exception

    // Verify handlers were called
    assertEquals(1, reRegisterHandler.getInvocationCount());
    assertEquals(1, mockCommandHandler.getInvocationCount());
    assertEquals(command1, reRegisterHandler.getLastCommand());
    assertEquals(command2, mockCommandHandler.getLastCommand());
  }

  @Test
  public void testHandlerThrowsException() {
    RuntimeException exception = new RuntimeException("Test exception");
    reRegisterHandler.setExceptionToThrow(exception);
    JobworkerCommand<?> command =
        new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand);

    dispatcher.handle(command);

    // reRegisterHandler should still be invoked
    assertEquals(1, reRegisterHandler.getInvocationCount());
    assertEquals(0, mockCommandHandler.getInvocationCount());
  }

  @Test
  public void testGetQueuedCommandCount() {
    // Set queued count in handlers
    reRegisterHandler.setQueuedCount(5);
    mockCommandHandler.setQueuedCount(3);
    Map<OMJobworkerCommandProto.Type, Integer> counts = dispatcher.getQueuedCommandCount();

    assertEquals(5, counts.get(OMJobworkerCommandProto.Type.reregisterCommand).intValue());
    assertEquals(3, counts.get(OMJobworkerCommandProto.Type.mockCommand).intValue());
    // Verify count for type without handler
    assertEquals(0,
        counts.getOrDefault(OMJobworkerCommandProto.Type.unknownCommand, 0).intValue());
  }

  @Test
  public void testGetCommandHandlerSummary() {
    dispatcher.handle(new MockJobworkerCommand(1L, OMJobworkerCommandProto.Type.reregisterCommand));
    dispatcher.handle(new MockJobworkerCommand(2L, OMJobworkerCommandProto.Type.reregisterCommand));
    dispatcher.handle(new MockJobworkerCommand(3L, OMJobworkerCommandProto.Type.mockCommand));

    Map<OMJobworkerCommandProto.Type, Integer> summary = dispatcher.getCommandHandlerSummary();

    assertEquals(2, summary.get(OMJobworkerCommandProto.Type.reregisterCommand).intValue());
    assertEquals(1, summary.get(OMJobworkerCommandProto.Type.mockCommand).intValue());
  }

  @Test
  public void testStop() {
    dispatcher.stop();
    // Verify stop was called on handlers
    verify(dispatcher, times(1)).stop();
    verify(dispatcher, times(1)).stop();
  }
}
