# Approval Workflow Example

This example demonstrates the Ballerina Temporal Workflow integration with a document approval workflow.

## Features Demonstrated

### 1. Per-Instance ServiceObject Isolation (Session 2)

Each workflow execution gets its own ServiceObject instance with independent state:

**Service-Level State Fields**:
- `workflowInstanceId` - Unique identifier for this workflow
- `initiatedBy` - User who started the workflow  
- `executionCount` - Number of times execute() was called (for replay tracking)

**Why This Matters**:
- Multiple concurrent workflows don't share state
- Replay scenarios get fresh ServiceObject instances
- No race conditions between workflow instances
- Clear separation of concerns

### 2. Activity Execution

Demonstrates calling activities from workflows with proper error handling.

### 3. Signal-Based Coordination

Shows how workflows can wait for external signals (approval/rejection) with timeouts.

### 4. Workflow Metadata Tracking

Uses service-level state to track workflow progression and context.

## Running the Example

### Prerequisites

1. Temporal server running on `localhost:7233`
2. Java 21 installed
3. Ballerina 2201.13.x installed

### Start the Service

```bash
# From module root
./build.sh

# Start the application
cd examples/approval
java -jar target/bin/approval.jar > /tmp/workflow-app.log 2>&1 &

# Monitor logs
tail -f /tmp/workflow-app.log
```

### Test Per-Instance State Isolation

Use the provided [tryit.http](tryit.http) file with VS Code REST Client extension:

#### Step 1: Start Multiple Concurrent Workflows

Execute these requests to start 3 concurrent workflow instances:

```http
POST http://localhost:9090/workflows/approval
Content-Type: */*

{"requestId": "REQ-2025-001", "amount": 1500, "requester": "alice@company.com"}
```

```http
POST http://localhost:9090/workflows/approval
Content-Type: */*

{"requestId": "REQ-2025-002", "amount": 3500, "requester": "bob@company.com"}
```

```http
POST http://localhost:9090/workflows/approval
Content-Type: */*

{"requestId": "REQ-2025-003", "amount": 750, "requester": "carol@company.com"}
```

#### Step 2: Observe Service-Level State in Logs

You should see output like:

```
[Workflow REQ-2025-001] Execution #1 initiated by alice@company.com
[Workflow REQ-2025-001] Processing approval for amount: 1500
[Workflow REQ-2025-002] Execution #1 initiated by bob@company.com
[Workflow REQ-2025-002] Processing approval for amount: 3500
[Workflow REQ-2025-003] Execution #1 initiated by carol@company.com
[Workflow REQ-2025-003] Processing approval for amount: 750
```

**Notice**: Each workflow maintains its own `workflowInstanceId` and `initiatedBy` values - no mixing!

#### Step 3: Signal Workflows to Complete

```http
POST http://localhost:9090/workflows/approval/signal
Content-Type: */*

{"requestId": "REQ-2025-001", "signalName": "approved", "comment": "approved by manager"}
```

#### Step 4: Verify Final State

Each workflow completes with its own isolated state:

```
[Workflow REQ-2025-001] Received signal: approved
[Workflow REQ-2025-001] Total executions in this instance: 1
Workflow REQ-2025-001 completed by alice@company.com after 1 execution(s)
```

### Understanding the Output

**Key Observations**:

1. **State Isolation**: Each workflow's logs show its unique `workflowInstanceId` and `initiatedBy`
2. **No Cross-Talk**: `REQ-2025-001` never shows data from `REQ-2025-002` or vice versa
3. **Execution Tracking**: `executionCount` increments during replay (if workflow is restarted)
4. **Concurrent Execution**: Multiple workflows run simultaneously without interference

### Architecture Insight

**Before (Session 1)**:
```
All Workflows → Single Shared ServiceObject
                     ↓
              State Conflicts Possible
```

**After (Session 2)**:
```
Workflow REQ-001 → ServiceObject Instance 1 (workflowInstanceId="REQ-001")
Workflow REQ-002 → ServiceObject Instance 2 (workflowInstanceId="REQ-002")
Workflow REQ-003 → ServiceObject Instance 3 (workflowInstanceId="REQ-003")
       ↓
Complete State Isolation
```

## Code Structure

```
approval/
├── main.bal                          # HTTP service endpoints
├── approval_workflow.bal             # Activity definitions
├── approval_workflow_service.bal     # Workflow service with state
├── tryit.http                        # Test scenarios
└── README.md                         # This file
```

## Testing Replay Scenarios

To test that replays get fresh ServiceObject instances:

1. Start a workflow
2. While it's waiting for signal, kill the application (`pkill -f approval.jar`)
3. Restart the application
4. Send the signal

You'll see `executionCount` increment on replay, but other state fields remain consistent with the original execution (deterministic replay).

## Cleanup

```bash
# Stop the application
pkill -f approval.jar

# Clean build artifacts
rm -rf target
```

## Further Reading

- [Main README](../../README.md) - Architecture and design decisions
- [agent.md](../../agent.md) - Development session notes
- [Session 2 Notes](../../agent.md#session-2-per-instance-serviceobject-architecture) - Per-instance ServiceObject implementation
