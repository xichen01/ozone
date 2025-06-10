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
package org.apache.hadoop.ozone.om.jobworker.command;

import static org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode.UNKNOWN_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandExecutionResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandResultCode;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.MockCommandResultsProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.jobworker.command.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker;
import org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.om.jobworker.node.StaleJobworkerHandler;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Integration test for OMJobworkerCommandManager with JobworkerCommandListener
 * and JobworkerCommandStatusReportHandler using the event system.
 */
public class TestOMJobworkerCommandManager {

  private JobworkerNodeManager nodeManager;
  private OMJobworkerCommandManager commandManager;
  private TestCommandListener testListener;
  private UUID jobworkerUuid;
  private JobworkerDetails jobworkerDetails;
  private EventQueue eventQueue;
  private JobworkerCommandStatusReportHandler commandStatusReportHandler;
  private final int timeoutCheckIntervalSeconds = 2;

  @BeforeEach
  public void setUp() {
    nodeManager = mock(JobworkerNodeManager.class);
    OMJobworkerCommandManager.CommandTimeoutChecker.setCheckIntervalForTesting(timeoutCheckIntervalSeconds);
    commandManager = new OMJobworkerCommandManager(nodeManager, "omServiceId");
    testListener = new TestCommandListener();
    jobworkerUuid = UUID.randomUUID();
    jobworkerDetails = MockJobworkerDetails.createJobworkerDetails(jobworkerUuid.toString());
    eventQueue = new EventQueue();
    commandStatusReportHandler = new JobworkerCommandStatusReportHandler(commandManager);
    StaleJobworkerHandler staleJobworkerHandler =
        new StaleJobworkerHandler(nodeManager, commandManager);
    eventQueue.addHandler(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT, commandStatusReportHandler);
    eventQueue.addHandler(OMJobworkerEvents.STALE_JOBWORKER, staleJobworkerHandler);

    // Register the test listener for both command types
    commandManager.registerHandler(OMJobworkerCommandProto.Type.reregisterCommand, testListener);
    commandManager.registerHandler(OMJobworkerCommandProto.Type.mockCommand, testListener);
  }

  @AfterEach
  public void tearDown() {
    commandManager.close();
  }

  @Test
  public void testCommandStateTransitions() throws IOException, InterruptedException, TimeoutException {
    OMJobworkerCommand command = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);

    long commandId = commandManager.sendCommand(jobworkerUuid, command);
    assertEquals(1L, commandId);
    verify(nodeManager).addOMJobworkerCommand(eq(jobworkerUuid), eq(command));

    waitAndAssert(testListener.sentLatch, testListener.sentCount, 1);

    // 1. PENDING -> EXECUTING
    CommandStatus executingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING);
    CommandStatusReportsProto executingReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(executingStatus)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, executingReport));

    // Wait for the handler to be called
    waitAndAssert(testListener.executingLatch, testListener.executingCount, 1);
    assertEquals(0, testListener.successCount.get());
    assertEquals(0, testListener.failureCount.get());
    assertEquals(1, testListener.sentCount.get());

    // 2. EXECUTING -> SUCCEEDED
    CommandStatus succeededStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED, null,
        null,
        CommandExecutionResultsProto.newBuilder()
            .setMockCommandResults(MockCommandResultsProto
                .newBuilder()
                .setResultCode(CommandResultCode.SUCCESS)
                .build())
            .build());
    CommandStatusReportsProto succeededReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(succeededStatus)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, succeededReport));

    // Wait for the handler to be called
    waitAndAssert(testListener.successLatch, testListener.successCount, 1);
    waitAndAssert(testListener.executingLatch, testListener.executingCount, 1);
    assertEquals(0, testListener.failureCount.get());
    assertEquals(1, testListener.sentCount.get());
    assertNotNull(testListener.lastExecutionResultsProto);
    assertEquals(CommandResultCode.SUCCESS,
        testListener.lastExecutionResultsProto.getMockCommandResults().getResultCode());
  }

  @Test
  public void testMultipleCommandStates() throws IOException, InterruptedException, TimeoutException {
    // Create multiple Command
    OMJobworkerCommand command1 = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);
    OMJobworkerCommand command2 = new MockOMJobworkerCommand(
        2L, OMJobworkerCommandProto.Type.reregisterCommand);
    OMJobworkerCommand command3 = new MockOMJobworkerCommand(
        3L, OMJobworkerCommandProto.Type.mockCommand);
    commandManager.sendCommand(jobworkerUuid, command1);
    commandManager.sendCommand(jobworkerUuid, command2);
    commandManager.sendCommand(jobworkerUuid, command3);

    GenericTestUtils.waitFor(() -> testListener.sentCount.get() == 3, 100, 5000);
    List<CommandStatus> statuses = Arrays.asList(
        // Command 1: PENDING -> EXECUTING
        createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING),

        // Command 2: PENDING -> EXECUTING
        createCommandStatus(2L, OMJobworkerCommandProto.Type.reregisterCommand, CommandStatus.Status.EXECUTING),

        // Command 3: PENDING -> FAILED (direct failure)
        createCommandStatus(3L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.FAILED)
    );

    CommandStatusReportsProto multiReport = CommandStatusReportsProto.newBuilder()
        .addAllCmdStatus(statuses)
        .build();

    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, multiReport));

    waitAndAssert(testListener.executingLatch, testListener.executingCount, 2);
    waitAndAssert(testListener.failureLatch, testListener.failureCount, 1);
    assertEquals(0, testListener.successCount.get());
    assertEquals(3, testListener.sentCount.get());


    CommandExecutionResultsProto failedResult = CommandExecutionResultsProto.newBuilder()
        .setMockCommandResults(MockCommandResultsProto
            .newBuilder()
            .setResultCode(CommandResultCode.BUCKET_NOT_FOUND)
            .build())
        .build();

    // Now update command states to final states
    List<CommandStatus> finalStatuses = Arrays.asList(
        // Command 1: EXECUTING -> SUCCEEDED
        createCommandStatus(1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED),


        // Command 2: EXECUTING -> FAILED
        createCommandStatus(2L, OMJobworkerCommandProto.Type.reregisterCommand, CommandStatus.Status.FAILED,
            "error msg", CommandResultCode.UNKNOWN_CODE, failedResult)
    );

    CommandStatusReportsProto finalReport = CommandStatusReportsProto.newBuilder()
        .addAllCmdStatus(finalStatuses)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, finalReport));

    // Wait for success handler to be called
    waitAndAssert(testListener.successLatch, testListener.successCount, 1);
    waitAndAssert(testListener.executingLatch, testListener.executingCount, 2); // Commands 1 and 2
    waitAndAssert(testListener.failureLatch, testListener.failureCount, 2);  // Commands 2 and 3
    assertEquals("error msg", testListener.lastCommandInfo.getMessage());
    assertEquals(CommandResultCode.UNKNOWN_CODE, testListener.lastCommandInfo.getResultCode());
    assertEquals(CommandResultCode.BUCKET_NOT_FOUND,
        testListener.lastExecutionResultsProto.getMockCommandResults().getResultCode());
    assertEquals(3, testListener.sentCount.get());
  }

  @Test
  public void testInvalidStateTransition() throws IOException, InterruptedException, TimeoutException {
    OMJobworkerCommand command = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);

    commandManager.sendCommand(jobworkerUuid, command);

    waitAndAssert(testListener.sentLatch, testListener.sentCount, 1);
    // First make a valid transition: PENDING -> EXECUTING
    CommandStatus executingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING);

    CommandStatusReportsProto validReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(executingStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, validReport));

    waitAndAssert(testListener.executingLatch, testListener.executingCount, 1);

    // Now try an invalid transition: EXECUTING -> PENDING (should not be allowed)
    CommandStatus pendingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.PENDING);

    CommandStatusReportsProto invalidReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(pendingStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, invalidReport));

    // Verify that the listener counts didn't change (invalid transition was ignored)
    waitAndAssert(testListener.executingLatch, testListener.executingCount, 1);
    assertEquals(0, testListener.successCount.get());
    assertEquals(0, testListener.failureCount.get());
    assertEquals(1, testListener.sentCount.get());

    // Now try a valid transition: EXECUTING -> SUCCEEDED
    CommandStatus succeededStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.SUCCEEDED);
    CommandStatusReportsProto finalReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(succeededStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, finalReport));

    // Verify that the success listener was called
    waitAndAssert(testListener.successLatch, testListener.successCount, 1);
    assertEquals(1, testListener.sentCount.get());
  }

  @Test
  public void testCommandStatusUpdateTimeout() throws IOException, InterruptedException, TimeoutException {
    // Set up a mock clock for the CommandTimeoutChecker to control time
    long initialTime = System.currentTimeMillis();
    AtomicLong mockClock = new AtomicLong(initialTime);
    OMJobworkerCommandManager.CommandTimeoutChecker timeoutChecker =
        commandManager.getCommandTimeoutChecker();
    timeoutChecker.setClock(mockClock::get);

    OMJobworkerCommand command = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);
    commandManager.sendCommand(jobworkerUuid, command);

    waitAndAssert(testListener.sentLatch, testListener.sentCount, 1);
    // Verify the command was added to the command info map
    Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> commandInfoMaps =
        commandManager.getCommandInfoMaps();
    Map<Long, JobworkerCommandInfo> mockCommandInfoMap =
        commandInfoMaps.get(OMJobworkerCommandProto.Type.mockCommand);
    assertTrue(mockCommandInfoMap.containsKey(1L));

    // Update the command to EXECUTING state
    CommandStatus executingStatus = createCommandStatus(
        1L, OMJobworkerCommandProto.Type.mockCommand, CommandStatus.Status.EXECUTING);
    CommandStatusReportsProto statusReport = CommandStatusReportsProto.newBuilder()
        .addCmdStatus(executingStatus)
        .build();
    eventQueue.fireEvent(OMJobworkerEvents.JW_COMMAND_STATUS_REPORT,
        new CommandStatusReportFromJobworker(jobworkerDetails, statusReport));
    waitAndAssert(testListener.executingLatch, testListener.executingCount, 1);

    // Before updated timeout, the command will not be lectured
    int checkedTimes = OMJobworkerCommandManager.CommandTimeoutChecker.getCheckTimes();
    Thread.sleep(timeoutCheckIntervalSeconds * 2 * 1000);
    assertTrue(OMJobworkerCommandManager.CommandTimeoutChecker.getCheckTimes() > checkedTimes);
    assertTrue(mockCommandInfoMap.containsKey(1L));

    long timeoutMs = timeoutChecker.getTimeoutMs();
    mockClock.set(initialTime + timeoutMs + 1000); // Add an extra second

    // Wait for the scheduled timeout checker to run
    waitAndAssert(testListener.timeoutLatch, testListener.timeoutCount, 1);
    // Command should be removed from a tracking map after timeout
    GenericTestUtils.waitFor(() -> !mockCommandInfoMap.containsKey(1L), 200, 3000);
    assertEquals(1, testListener.sentCount.get());
  }

  @Test
  public void testStaleJobworkerMarksCommandsFailed()
      throws IOException, InterruptedException, TimeoutException {
    // Create multiple mock commands for different command types
    OMJobworkerCommand command1 = new MockOMJobworkerCommand(
        1L, OMJobworkerCommandProto.Type.mockCommand);
    OMJobworkerCommand command2 = new MockOMJobworkerCommand(
        2L, OMJobworkerCommandProto.Type.reregisterCommand);
    OMJobworkerCommand command3 = new MockOMJobworkerCommand(
        3L, OMJobworkerCommandProto.Type.mockCommand);

    commandManager.sendCommand(jobworkerUuid, command1);
    commandManager.sendCommand(jobworkerUuid, command2);
    commandManager.sendCommand(jobworkerUuid, command3);

    GenericTestUtils.waitFor(() -> testListener.sentCount.get() == 3, 100, 5000);
    Mockito.when(nodeManager.pollJobworkerCommand(jobworkerUuid))
        .thenReturn(Arrays.asList(command1, command2, command3));
    Map<OMJobworkerCommandProto.Type, Map<Long, JobworkerCommandInfo>> commandInfoMaps =
        commandManager.getCommandInfoMaps();
    Map<Long, JobworkerCommandInfo> mockCommandInfoMap =
        commandInfoMaps.get(OMJobworkerCommandProto.Type.mockCommand);
    Map<Long, JobworkerCommandInfo> reregisterCommandInfoMap =
        commandInfoMaps.get(OMJobworkerCommandProto.Type.reregisterCommand);
    assertTrue(mockCommandInfoMap.containsKey(1L));
    assertTrue(mockCommandInfoMap.containsKey(3L));
    assertTrue(reregisterCommandInfoMap.containsKey(2L));

    eventQueue.fireEvent(OMJobworkerEvents.STALE_JOBWORKER, jobworkerDetails);

    waitAndAssert(testListener.failureLatch, testListener.failureCount, 3);

    // Retrieve command info maps
    // Verify that all commands are marked as failed and removed from the mapping
    assertFalse(mockCommandInfoMap.containsKey(1L));
    assertFalse(mockCommandInfoMap.containsKey(3L));
    assertFalse(reregisterCommandInfoMap.containsKey(2L));
    assertEquals(3, testListener.sentCount.get());
  }

  /**
   * Test implementation of JobworkerCommandListener.
   */
  private static class TestCommandListener implements JobworkerCommandListener {
    private final AtomicInteger sentCount = new AtomicInteger(0);
    private final AtomicInteger executingCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger timeoutCount = new AtomicInteger(0);

    private final CountDownLatch sentLatch = new CountDownLatch(1);
    private final CountDownLatch executingLatch = new CountDownLatch(1);
    private final CountDownLatch successLatch = new CountDownLatch(1);
    private final CountDownLatch failureLatch = new CountDownLatch(1);
    private final CountDownLatch timeoutLatch = new CountDownLatch(1);
    private volatile CommandExecutionResultsProto lastExecutionResultsProto = null;
    private volatile JobworkerCommandInfo lastCommandInfo = null;

    @Override
    public void onSendCommand(OMJobworkerCommand command, UUID jobworkerUuid) {
      sentCount.incrementAndGet();
      sentLatch.countDown();
    }

    @Override
    public void onCommandSucceeded(JobworkerCommandInfo statusInfo,
        CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
      successCount.incrementAndGet();
      successLatch.countDown();
      lastExecutionResultsProto = executionResultsProto;
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onCommandFailed(JobworkerCommandInfo statusInfo,
        CommandExecutionResultsProto executionResultsProto, JobworkerDetails jobworkerDetails) {
      failureCount.incrementAndGet();
      failureLatch.countDown();
      lastExecutionResultsProto = executionResultsProto;
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onCommandExecuting(JobworkerCommandInfo statusInfo,
                                   JobworkerDetails jobworkerDetails) {
      executingCount.incrementAndGet();
      executingLatch.countDown();
      lastCommandInfo = statusInfo;
    }

    @Override
    public void onStatusUpdateTimeout(JobworkerCommandInfo statusInfo, UUID jobworkerUuid) {
      timeoutCount.incrementAndGet();
      timeoutLatch.countDown();
      lastCommandInfo = statusInfo;
    }
  }

  private CommandStatus createCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
      CommandStatus.Status status) {
    return createCommandStatus(cmdId, type, status, null, null, null);
  }

  private CommandStatus createCommandStatus(long cmdId, OMJobworkerCommandProto.Type type,
      CommandStatus.Status status, String msg, CommandResultCode resultCode,
      CommandExecutionResultsProto commandExecutionResults) {
    CommandStatus.Builder builder = CommandStatus.newBuilder()
        .setCmdId(cmdId)
        .setType(type)
        .setStatus(status)
        .setOmServiceId("test-service");
    if (commandExecutionResults != null) {
      builder.setExecutionResults(commandExecutionResults);
    }
    if (msg != null) {
      builder.setMsg(msg);
    }
    if (resultCode != null) {
      builder.setResultCode(resultCode);
    }
    return builder.build();
  }

  /**
   * Wait for a latch and assert the counter value.
   *
   * @param latch         the latch to wait for
   * @param counter       the counter to check
   * @param expectedValue the expected value of the counter
   * @throws InterruptedException if interrupted while waiting
   */
  private void waitAndAssert(CountDownLatch latch, AtomicInteger counter,
                             int expectedValue) throws InterruptedException, TimeoutException {
    assertTrue(latch.await(10, TimeUnit.SECONDS),
        "Latch should have been released within " + 10 + " seconds");
    if (expectedValue != counter.get()) {
      GenericTestUtils.waitFor(() -> expectedValue == counter.get(), 500, 5000);
    }
  }
}