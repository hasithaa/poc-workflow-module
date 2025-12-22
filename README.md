# Ballerina Temporal Workflow Module

A comprehensive Ballerina integration for Apache Temporal that provides durable, distributed workflow orchestration capabilities.

## Overview

This module enables building resilient workflows with Temporal's orchestration engine while leveraging Ballerina's type-safe, concurrent programming model. Key features include:

- **Activity Execution**: Durable execution of long-running operations with automatic retry handling
- **Signal Handling**: Dynamic signal-based workflow control with flexible data types (`map<anydata>`)
- **Error Handling**: Proper distinction between Ballerina error returns (valid results) and exceptions (failures)
- **Type Safety**: Explicit anydata typing for all map-based data exchanges
- **Workflow Replay**: Deterministic replay with consistent result serialization

## Architecture

### Core Components

1. **WorkflowWorkerNative** (Java)
   - Manages Temporal worker and activity registration
   - Converts between Ballerina and Temporal types
   - Handles activity execution with proper error semantics
   - Implements dynamic activity adapter for type-agnostic activity calls
   - **Per-Instance ServiceObjects**: Creates a fresh ServiceObject for each workflow execution using `ValueCreator.createObjectValue()` with type information from the registered template

2. **WorkflowContextNative** (Java)
   - Provides activity execution interface via untyped ActivityStub
   - Manages signal awaiting and correlation data
   - Handles Ballerina ↔ Java type conversions with explicit TYPE_ANYDATA
   - Supports workflow replica awareness and condition waiting
   - Independent lifecycle from ServiceObject (one context per workflow execution)

3. **Context** (Ballerina Client Class)
   - Injected into workflow execute methods
   - Public API: `callActivity()`, `awaitSignal()`, `awaitAnySignal()`, `sleep()`, `isReplaying()`
   - Returns `map<anydata>` for flexible signal and result data handling
   - Thread-safe with synchronized method access

### ServiceObject Lifecycle

**Template Registration** (at service attachment):
- Original ServiceObject stored in `SERVICE_REGISTRY` as a template
- Template contains type information used for creating instances

**Per-Execution Instance Creation** (at workflow start):
1. Retrieve template from `SERVICE_REGISTRY.get(workflowType)`
2. Extract type information: `templateService.getType()`
3. Create new instance: `ValueCreator.createObjectValue(serviceType.getPackage(), serviceType.getName())`
4. Each workflow execution (including replays) gets its own isolated instance

**Benefits**:
- **State Isolation**: No shared state between workflow instances
- **Replay Safety**: Fresh instances on replay prevent state corruption
- **Thread Safety**: No need for synchronization across workflow instances
- **Determinism**: Each execution starts with clean state

### Thread Model

**Critical: Non-Blocking Signal Waits**

When `Workflow.await()` is called for signal waiting:
1. Temporal captures the workflow state (continuation)
2. Execution yields back to Temporal (thread released)
3. Workflow state persisted in history
4. Thread returns to pool for other work
5. When condition met, workflow resumes from checkpoint

**Implications**:
- ✅ No Ballerina scheduler threads blocked during waits
- ✅ Workflows can wait hours/days without resource consumption
- ✅ Thousands of concurrent waiting workflows possible
- ✅ During replay, waits complete instantly if condition already met
- ✅ Thread active only during actual code execution

This is fundamentally different from traditional thread blocking - it uses coroutine/continuation semantics similar to async/await patterns.

### Error Handling Model

**Critical Design Philosophy**: Ballerina error returns are treated as valid results, not failures.

```java
Activity Execution Result Processing:
├─ BError returned    → Serialize to error map (NOT thrown) → Normal completion
├─ Exception thrown   → Propagate naturally → Temporal failure (retry)
└─ Result returned    → Serialize normally → Normal completion

Error Map Structure:
{
  "__error__": true,
  "message": "error message string",
  "details": { /* nested anydata */ }
}
```

**Rationale**: 
- Java exceptions represent uncontrolled errors (panics in Ballerina)
- Ballerina error returns represent controlled, expected error conditions
- Temporal retries only for true failures (exceptions/panics)
- Maintains deterministic replay history

## Build Steps

### Prerequisites

- **Java**: 21 (virtual threads support)
- **Ballerina**: 2201.13.x or later (anydata support)
- **Maven**: 3.x
- **Temporal Server**: Running on localhost:7233

### Full Build Process

```bash
# 1. Build Java native implementations
cd native
mvn clean package

# 2. Build and push Ballerina module
cd ../ballerina
bal build
bal pack
bal push --repository=local

# 3. Build example (if changed)
cd ../examples/approval
rm -rf target
bal build

# Output: target/bin/approval.jar
```

### Start the Application

```bash
cd examples/approval

# Run in background with logging
java -jar target/bin/approval.jar > /tmp/workflow-app.log 2>&1 &

# Monitor logs in real-time
tail -f /tmp/workflow-app.log

# Service endpoints:
# HTTP API: http://localhost:9090
# Temporal: localhost:7233
# Task queues: approval-processing
```

### Stop the Application

```bash
# Kill the running process
pkill -f "approval.jar"

# Or by PID
ps aux | grep "approval.jar"
kill <PID>
```

### Development Cycle

When making code changes:

```bash
# Full rebuild with cleanup
pkill -f "approval.jar"

cd native
mvn clean package -DskipTests

cd ../ballerina
bal pack && bal push --repository=local

cd ../examples/approval
rm -rf target && bal build

# Restart
java -jar target/bin/approval.jar > /tmp/workflow-app.log 2>&1 &
```

## API Usage

### Workflow Execution

```ballerina
public function execute(Context ctx, ApprovalRequest request) returns error? {
    // Call an activity with retry semantics
    ApprovalResult result = check ctx->callActivity("approveRequest", request);
    
    // Wait for approval signal (any value type)
    map<anydata> signalData = check ctx->awaitSignal("approval", 3600);
    
    // Wait for any of multiple signals
    SignalResult|error firstSignal = ctx->awaitAnySignal(
        ["approved", "rejected", "escalated"],
        7200  // 2 hour timeout
    );
    
    // Sleep (durable delay)
    check ctx->sleep(60);  // 60 seconds
    
    // Check if currently replaying history
    if !ctx->isReplaying() {
        io:println("First execution (not replay)");
    }
    
    return ();
}
```

### Query Workflow State

```ballerina
service "MyWorkflow" on listener {
    private string status = "pending";
    
    # Execute workflow
    isolated remote function execute(Context ctx, string id) returns string|error {
        self.status = "processing";
        // ... workflow logic ...
        self.status = "completed";
        return "Done";
    }
    
    # Query current status - read-only operation
    isolated remote function getStatus() returns map<anydata> {
        return {
            "status": self.status,
            "timestamp": time:utcNow()
        };
    }
}
```

### Call Query from Client

```ballerina
// Query workflow state
anydata result = check workflowClient->query(
    {
        "workflowType": "MyWorkflow",
        "id": "workflow-123"
    },
    "getStatus"  // Query method name
);

// Use query result
map<anydata> status = check result.ensureType();
io:println("Workflow status: " + status["status"].toString());
```

### Signal Sending

```ballerina
import ballerina/http;
import hasitha/workflow as wf;

service / on new http:Listener(9090) {
    private final wf:Client workflowClient;
    
    resource function post workflows/signal(
        string workflowId,
        string signalName,
        map<anydata> signalData
    ) returns error? {
        check self.workflowClient->sendSignal(
            workflowId,
            signalName,
            signalData
        );
    }
}
```

### Activity Implementation

```ballerina
public isolated function approveRequest(ApprovalRequest request) returns ApprovalResult|error {
    // Regular activity - may return error or result
    if request.amount > 100000 {
        // Return error as normal result (not thrown)
        return error("Amount exceeds limit");
    }
    
    // Call external service
    ApprovalResult result = check callExternalService(request);
    return result;
}
```

## Recent Updates

### December 22, 2025 - Session 3

#### Query Feature Implementation
- **Query Support**: Implemented `DynamicQueryHandler` for read-only workflow state inspection
- Client API: Added `workflowClient->query()` method
- Service Methods: Query methods can return any `map<anydata>` or serializable type
- Synchronous execution - returns immediately without workflow modification
- Not recorded in workflow history (read-only operations)
- Useful for dashboards, monitoring, and status checks

**Example Use Cases**:
- Check workflow status while waiting for signals
- Monitor progress without interfering with execution
- Build real-time dashboards
- Debugging and observability

### December 22, 2025 - Session 2

#### Per-Instance ServiceObject Architecture
- **ServiceObject Isolation**: Each workflow execution now gets its own ServiceObject instance
- Implemented `createServiceInstance()` method using Ballerina runtime APIs
- Uses `ValueCreator.createObjectValue()` with type information from template service
- Applies to both initial execution and replay scenarios
- Prevents state sharing between concurrent workflow instances

#### Thread Model Clarification
- **Non-Blocking Waits**: Documented that `Workflow.await()` uses coroutines, not thread blocking
- Signal/condition waiting releases calling thread back to pool
- Workflow state persisted during waits, resumed when condition met
- No Ballerina scheduler threads held unnecessarily
- Enables efficient resource usage for long-running workflows

**See detailed technical notes in session documentation below**

### December 22, 2025 - Session 1

#### Type System Enhancement
- **All APIs now use `map<anydata>`** for flexible heterogeneous data
- Implemented `TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA)` pattern
- Updated `SignalResult` record to support diverse signal payload types
- Removed string-only restrictions on signal data

#### Critical Error Handling Fix
- **BError serialization**: Activities returning `BError` no longer trigger `ApplicationFailure`
- Errors serialized to maps with `__error__: true` marker
- Maintains deterministic history across retries
- Only Java exceptions cause Temporal retries

#### API Changes
- `Context.awaitSignal()` returns `map<anydata>` (was `map<string>`)
- `Context.awaitAnySignal()` returns `SignalResult` with `map<anydata> data`
- `ContextNative.recordSignal()` accepts `BMap<BString, Object>` parameters

**See [agent.md](./agent.md) for detailed technical session notes**

## Type Mappings

| Ballerina | Java | Temporal | Notes |
|-----------|------|----------|-------|
| map<anydata> | BMap<BString, Object> | Map<String, Object> | Flexible data structure |
| int | long | Long | 64-bit integer |
| string | BString → String | String | UTF-8 encoded |
| error | BError | error-map | Contains `__error__`, `message`, `details` |
| anydata[] | BArray | Object[] | Heterogeneous array |
| record | BObject | nested-map | Flattened to map |
| function | BFunctionPointer | Java lambda | Via FPValue |

## File Structure

```
module-hasitha-workflow/
├── native/                          # Java implementation
│   ├── src/main/java/.../
│   │   ├── WorkflowWorkerNative.java      # Worker & activity adapter
│   │   ├── WorkflowContextNative.java     # Context implementation
│   │   ├── ContextNative.java             # Signal handling
│   │   ├── SignalAwaitWrapper.java        # Signal queuing
│   │   └── TypesUtil.java                 # Type conversions
│   └── pom.xml
├── ballerina/                       # Public APIs
│   ├── context.bal                  # Context class (injected)
│   ├── client.bal                   # Workflow client
│   ├── listener.bal                 # Listener for services
│   ├── types.bal                    # SignalResult, etc.
│   └── Ballerina.toml
├── examples/
│   └── approval/                    # Reference workflow
│       ├── main.bal                 # Entry point
│       ├── approval_workflow.bal    # Workflow logic
│       └── Config.toml
├── agent.md                         # Development session notes
└── README.md                        # This file
```

## Troubleshooting

### Build Issues

**Problem**: Maven compilation fails
```bash
# Solution: Clean and rebuild
cd native
mvn clean package -DskipTests
```

**Problem**: Ballerina dependency conflicts
```bash
# Solution: Clear cache and rebuild
cd ballerina
rm -rf target .ballerina
bal build --offline
```

### Runtime Issues

**Problem**: Type errors in signal data
```
Error: Type mismatch for signal data
```
**Solution**: Ensure signal data conforms to `map<anydata>` structure. All values must be serializable.

**Problem**: Activity timeout or retry failures
```
ERROR: Activity execution failed with timeout
```
**Solution**: 
- Check activity implementation for exception throws
- Verify error returns use proper error format
- Review Temporal server logs for scheduling issues

**Problem**: Workflow replay issues
```
ERROR: Event history mismatch during replay
```
**Solution**:
- Ensure activity results maintain consistent structure
- Check for non-deterministic code in workflows
- Verify signal data types don't change between executions

### Type Checking

**Problem**: Map type compatibility
```ballerina
error: expected 'map<string>' but found 'map<anydata>'
```
**Solution**: Update type annotations to use `map<anydata>` throughout

## Development Guidelines

### When Modifying the Module

1. **Update `agent.md`** with development notes and session summary
2. **Ensure all BMap creation** uses `TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA)`
3. **Test error returns** vs. exception throwing in activities
4. **Verify signal data** type flexibility with complex structures
5. **ServiceObject isolation**: Remember each workflow gets its own instance - avoid assumptions about shared state
6. **Test replay scenarios**: Verify new ServiceObject instances work correctly during replay
7. **Run full build** before committing: `./build.sh`

### Code Patterns

**Correct Map Creation**:
```java
BMap<BString, Object> map = ValueCreator.createMapValue(
    TypeCreator.createMapType(PredefinedTypes.TYPE_ANYDATA));
```

**Correct Error Handling**:
```java
if (result instanceof BError) {
    BError error = (BError) result;
    // Serialize, don't throw
    Map<String, Object> errorMap = new HashMap<>();
    errorMap.put("__error__", true);
    errorMap.put("message", error.getMessage());
    return errorMap;  // Normal return
}
```

**Correct Type Conversion**:
```java
Object ballerinaValue = TypesUtil.convertJavaToBallerinaType(javaValue);
Object javaValue = WorkflowWorkerNative.convertBallerinaToJavaType(ballerinaValue);
```

## Performance Considerations

- **Virtual Threads**: Activities run on Java 21 virtual threads for optimal concurrency
- **Type Conversion Overhead**: Minimal - happens at activity boundaries only
- **Signal Queuing**: Lock-free for high throughput signal processing
- **Map Serialization**: Lazy evaluation where possible

## Known Limitations

1. **Nested Error Details**: Currently simplified during serialization (stored as `details` map)
2. **Custom Types**: Require manual conversion to `map<anydata>`
3. **Large Workflows**: No built-in pagination for long histories
4. **Clock Skew**: Requires synchronized temporal servers

## Contributing

Submit improvements via pull requests. Ensure:
- Full build passes (`./build.sh`)
- Session notes in `agent.md`
- Type safety maintained (use TYPE_ANYDATA)
- Error vs. exception semantics preserved

---

**Last Updated**: December 22, 2025  
**Repository**: hasithaa/poc-workflow-module  
**Branch**: main  
**Ballerina Version**: 2201.13.x  
**Java Version**: 21  
**Temporal SDK**: 1.32.1
