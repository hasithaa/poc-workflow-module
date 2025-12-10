## Build Steps

### Prerequisites

- Java 21
- Ballerina 2201.12.7 or later
- Maven 3.x
- Temporal Server running on localhost:7233

### Full Build Process

```bash
# 1. Build Java native implementations
cd native
mvn clean package

cd ../ballerina
bal build
bal pack
bal push --repository=local

# If example changed or dependencies updated:
cd ../examples/approval && rm -rf target && bal build

# Output: target/bin/e1.jar
```

### Start the Application (Background Mode)

```bash
cd examples/approval

# Start in background with logs redirected
java -jar target/bin/e1.jar > /tmp/workflow-app.log 2>&1 &

# Check logs
tail -f /tmp/workflow-app.log

# HTTP service starts on: http://localhost:9090
# Workers start for task queues: approval-processing
```

### Stop the Application

```bash
# Kill the running process
pkill -f "e1.jar"

# Or find and kill by PID
ps aux | grep "e1.jar"
kill <PID>
```

### Rebuild and Restart Workflow

When making changes, always kill the running process before rebuilding:

```bash
# Full rebuild cycle
pkill -f "e1.jar"
cd native
mvn clean package -DskipTests
cd ../ballerina
bal pack && bal push --repository=local
cd ../examples/approval
rm -rf target && bal build
java -jar target/bin/e1.jar > /tmp/workflow-app.log 2>&1 &
```

### Test Workflows

```bash
# Start an approval workflow
curl -X POST http://localhost:9090/workflows/approval \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "REQ-001",
    "amount": 5000,
    "requester": "user@example.com"
  }'

# Response: {"status":"success", "workflowId":"ApprovalWorkflow-REQ-001", ...}

# Send approval signal
curl -X POST http://localhost:9090/workflows/approval/signal/ \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "REQ-001",
    "signalName": "approved",
    "comment": "approved by manager"
  }'

# Check logs for workflow execution
tail -30 /tmp/workflow-app.log | grep -E "(Starting|completed|ERROR)"
```