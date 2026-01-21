# Workflow Module V0

A simplified workflow orchestration module for Ballerina, providing durable execution capabilities backed by Temporal.

## Features

- **Durable Workflows**: Workflows survive process restarts and failures
- **Activity Execution**: Execute activities with automatic retry and replay protection
- **Signal Handling**: Receive external events and data during workflow execution
- **Sleep/Timers**: Durable delays that survive process restarts
- **Simple API**: No listener pattern, direct engine-based workflow management

## Quick Start

```ballerina
import hasitha/workflow_v0;

// Define your workflow service
service class MyWorkflow {
    *workflow_v0:WorkflowService;
    
    isolated remote function execute(workflow_v0:WFContext ctx, string input) returns string|error {
        // Your workflow logic here
        return "Done";
    }
}

// Start the engine and register workflow
public function main() returns error? {
    workflow_v0:Engine engine = check new ({
        serviceUrl: "localhost:7233",
        taskQueue: "my-queue"
    });
    
    check engine.register("MyWorkflow", MyWorkflow);
    workflow_v0:GenericClient client = check engine.getClient();
    
    workflow_v0:WorkflowData data = check client->startWorkflow("MyWorkflow", "input");
    
    // Start processing (blocking)
    check engine.startListen();
}
```
