# Error Handling Semantics in Ballerina Workflow Module

## Overview

The Ballerina Workflow module maps Ballerina error semantics to Temporal workflow failure modes. This design ensures that business logic errors and system failures are handled appropriately.

---

## 🗺️ Semantic Mapping: Ballerina to Temporal

| Ballerina Error Semantic | Temporal Failure Type | Temporal Workflow Status | What it Signifies |
|:---|:---|:---|:---|
| **1. Returned Error**<br/>`return error("Invalid input");` | **Workflow Execution Failure** | Final Status: **`FAILED`** | **Business Error:** The workflow successfully executed its logic and determined the outcome is a business failure. The workflow state is clean and complete. |
| **2. Panic Error**<br/>`panic error("Bug detected");` | **Workflow Task Failure** | Intermediate Status: **`RUNNING`**<br/>(with retries) | **System Error/Non-Deterministic Bug:** An unexpected runtime exception occurred. Temporal cannot trust the worker's state and will retry the task. |

---

## Detailed Behavior

### 1. Returned Error → Workflow Execution Failure (`FAILED`)

**Use Case:** Business logic determines the workflow cannot proceed successfully.

#### Example Scenarios:
- Insufficient funds in a payment workflow
- Input data validation failed
- Activity returned an error indicating business logic failure
- Authorization denied

#### Implementation:
```ballerina
isolated remote function execute(workflow:Context ctx, string orderId) returns string|error {
    // Activity returns error for business reasons
    var result = check ctx->callActivity("processPayment", orderId);
    
    // Workflow logic determines failure
    if (amount < minimumAmount) {
        return error("Amount below minimum threshold");
    }
    
    return "Success";
}
```

#### What Happens:
1. The workflow function returns an `error` value
2. The Ballerina Workflow adapter detects the returned error
3. Temporal marks the **Workflow Execution** as **`FAILED`**
4. The workflow history is complete and sealed
5. No retries occur - this is a terminal state

#### Temporal UI:
- **Workflow Status:** `FAILED`
- **Event History:** Shows `WorkflowExecutionFailed` event with the error message
- **Result:** Clean failure with business error message

---

### 2. Panic Error → Workflow Task Failure (Retries)

**Use Case:** System errors, crashes, or non-deterministic bugs in the worker.

#### Example Scenarios:
- Worker process crashes during execution
- Null pointer exceptions / runtime panics
- Non-deterministic code (different results on replay)
- Out of memory errors
- Network failures in the worker

#### Implementation:
```ballerina
isolated remote function execute(workflow:Context ctx, string orderId) returns string|error {
    // System error - this will panic
    string? nullValue = ();
    string result = nullValue.toString(); // Runtime panic!
    
    return result;
}
```

#### What Happens:
1. The Ballerina runtime encounters an unhandled panic
2. The exception bubbles up to the Temporal SDK
3. Temporal records a **`WorkflowTaskFailed`** event
4. The workflow status remains **`RUNNING`**
5. Temporal **retries** the workflow task on another worker
6. The workflow continues retrying until:
   - The bug is fixed and the worker succeeds
   - Manual intervention terminates the workflow

#### Temporal UI:
- **Workflow Status:** `RUNNING` (not failed)
- **Event History:** Shows `WorkflowTaskFailed` events with retry attempts
- **Result:** Workflow stays in running state, awaiting successful execution

---

## Activity Error Handling

Activities have similar semantics but with an additional distinction:

### 1. Activity Returns Error (Business Error)

**Behavior:** Activity completes successfully, returns error value to workflow.

```ballerina
isolated function validateDocument(string docId) returns string|error {
    if (!isValid(docId)) {
        return error("Document validation failed"); // Business error
    }
    return "Valid";
}
```

**Result:**
- Activity completes in Temporal (status: `COMPLETED`)
- Error value is serialized and returned to workflow
- Workflow receives the error and can handle it with `check`
- If workflow uses `check`, the workflow fails (Workflow Execution Failure)
- **No Temporal retries** - this is not an activity failure

### 2. Activity Panics (System Error)

**Behavior:** Activity fails in Temporal, triggers retry logic.

```ballerina
isolated function processPayment(string orderId) returns string|error {
    // System failure
    panic error("Database connection lost"); // System error
}
```

**Result:**
- Activity fails in Temporal (status: `FAILED`)
- Temporal records `ActivityTaskFailed` event
- Temporal **retries** the activity based on retry policy
- After max retries, activity failure propagates to workflow
- Workflow task may also fail if activity failure is not handled

---

## Best Practices

### ✅ Use Returned Errors For:
- **Business validation failures** ("Invalid input", "Insufficient funds")
- **Authorization/Permission denials** ("User not authorized")
- **Expected operational failures** ("Service unavailable - do not retry")
- **Data not found scenarios** ("Order not found")

### ✅ Use Panic For:
- **True system errors** (should be rare in production code)
- **Programming bugs** (null pointers, type mismatches)
- **Non-deterministic behavior** (random values, timestamps in workflow code)
- **Fatal worker issues** (out of memory, corrupted state)

### ⚠️ Anti-Patterns to Avoid:

❌ **Don't panic for business errors:**
```ballerina
// BAD - This will cause retries
if (amount < 0) {
    panic error("Negative amount"); // Wrong! Use return error
}
```

❌ **Don't return errors for system failures:**
```ballerina
// BAD - This won't retry the activity
isolated function fetchData() returns json|error {
    json|error result = http->get("/api/data");
    if result is error {
        return error("Network error"); // Wrong! Should let it panic/retry
    }
    return result;
}
```

✅ **Correct approach:**
```ballerina
// GOOD - Business error as return value
if (amount < 0) {
    return error("Negative amount not allowed");
}

// GOOD - Let system errors propagate
isolated function fetchData() returns json|error {
    // Network errors will naturally cause retries
    return check http->get("/api/data");
}
```

---

## Configuration

### Activity Retry Policy
Activities have retry configuration to handle transient failures:

```ballerina
// Current default configuration
ActivityOptions activityOptions = ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofMinutes(2))
    .setRetryOptions(RetryOptions.newBuilder()
        .setMaximumAttempts(1) // Current setting: 1 attempt only
        .build())
    .build();
```

**Note:** The current implementation has `maxAttempts=1`, meaning activities won't retry on panic. Consider increasing this for production to handle transient failures.

### Workflow Retry Policy
Workflow task failures (panics) are automatically retried by Temporal with infinite attempts by default.

---

## Implementation Details

### Error Serialization
When an activity returns an error:
1. The `BError` is converted to a serializable map:
   ```java
   {
       "__error__": true,
       "message": "Error message here",
       "details": {...}
   }
   ```
2. This map is stored in Temporal's event history
3. When the workflow receives it, the map is converted back to a Ballerina error
4. The workflow can handle it using `check` or explicit error handling

### Panic Handling
When code panics:
1. The exception bubbles up to the Temporal SDK
2. Temporal records a `WorkflowTaskFailed` or `ActivityTaskFailed` event
3. The task is automatically retried according to retry policies
4. The workflow or activity history shows all retry attempts

---

## Testing Recommendations

### Test Business Errors:
```ballerina
@test:Config {}
function testBusinessError() {
    // Verify workflow returns error for business logic failures
    string|error result = workflow->execute(...);
    test:assertTrue(result is error);
    test:assertEquals((<error>result).message(), "Expected business error");
}
```

### Test System Resilience:
- Inject transient failures (network issues, temporary unavailability)
- Verify activities retry appropriately
- Ensure workflow task failures trigger retries
- Test non-deterministic code detection

---

## Monitoring and Debugging

### Workflow Failed (Business Error):
- **Status:** `FAILED`
- **Check:** Event history for `WorkflowExecutionFailed` event
- **Action:** Review business logic, may be expected behavior

### Workflow Running with Task Failures (System Error):
- **Status:** `RUNNING`
- **Check:** Event history for `WorkflowTaskFailed` events
- **Action:** Investigate worker logs, fix bugs, restart workers

### Activity Failed:
- **Status:** Activity marked as `FAILED` in history
- **Check:** Event history for `ActivityTaskFailed` events
- **Action:** Check if retries exhausted, review activity logs

---

## Summary

| Scenario | Ballerina Construct | Temporal Outcome | Retries? |
|:---|:---|:---|:---|
| Business logic failure | `return error(...)` | Workflow `FAILED` | ❌ No |
| System error/bug | `panic error(...)` | Workflow `RUNNING` + Task Failure | ✅ Yes |
| Activity business error | Activity `return error(...)` | Activity `COMPLETED`, Workflow handles error | ❌ No |
| Activity system error | Activity `panic error(...)` | Activity `FAILED` | ✅ Yes (if configured) |

This design ensures clean separation between expected business failures and unexpected system errors, giving you precise control over workflow behavior.
