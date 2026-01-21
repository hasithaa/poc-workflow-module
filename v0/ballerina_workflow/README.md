# Workflow Module V0

A simplified workflow orchestration module for Ballerina, providing durable execution capabilities backed by Temporal.

## Overview

This module provides a simple API for building durable, long-running workflows with Ballerina. Workflows can:
- Execute activities with automatic retry
- Sleep/delay durably across process restarts
- Receive external signals during execution
- Handle complex business processes reliably

## Installation

Add the dependency to your `Ballerina.toml`:

```toml
[[dependency]]
org = "hasitha"
name = "workflow_v0"
version = "0.1.0"
```

## Quick Start

### Define a Workflow Service

```ballerina
import hasitha/workflow_v0;

service class MyWorkflow {
    *workflow_v0:WorkflowService;
    
    isolated remote function execute(workflow_v0:WFContext ctx, string input) returns string|error {
        // Execute an activity
        string result = check ctx->callActivity(self.processData, string, input);
        
        // Durable sleep
        check ctx->sleep({hours: 1});
        
        // Wait for signal
        string approval = check ctx->awaitSignal("approve", string, 300);
        
        return "Completed: " + approval;
    }
    
    isolated function processData(string data) returns string {
        return "Processed: " + data;
    }
    
    @workflow_v0:Signal
    remote isolated function approve(string decision) returns string {
        return decision;
    }
}
```

### Start and Interact with Workflows

```ballerina
public function main() returns error? {
    // Initialize engine
    workflow_v0:Engine engine = check new ({
        serviceUrl: "localhost:7233",
        taskQueue: "my-queue"
    });
    
    // Register workflow
    check engine.register("MyWorkflow", MyWorkflow);
    
    // Get client
    workflow_v0:GenericClient client = check engine.getClient();
    
    // Start workflow
    workflow_v0:WorkflowData data = check client->startWorkflow("MyWorkflow", "input");
    
    // Send signal
    check client->sendSignal("MyWorkflow", data.workflowId, "approve", "approved");
    
    // Start worker (blocking)
    check engine.startListen();
}
```

## API Reference

### Engine

Main class for workflow orchestration:
- `init(TemporalConfig)` - Initialize with configuration
- `register(workflowName, ServiceTypedesc)` - Register workflow
- `getClient()` - Get client for workflow operations
- `startListen()` - Start worker (blocking)
- `stop()` - Stop worker gracefully

### GenericClient  

Client for workflow operations:
- `startWorkflow(workflowName, ...params)` - Start workflow instance
- `sendSignal(workflowName, workflowId, signalName, ...params)` - Send signal
- `query(workflowName, workflowId, queryName)` - Query workflow state

### WFContext

Workflow execution context:
- `callActivity(func, T, ...args)` - Execute activity
- `awaitSignal(signalName, T, timeout)` - Wait for signal
- `sleep(duration)` - Durable delay

## Requirements

- Temporal server running (default: localhost:7233)
- Java 21 or higher
- Ballerina 2201.10.2 or higher

## License

Apache License 2.0
