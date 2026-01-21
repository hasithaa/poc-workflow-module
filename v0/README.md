# Workflow Module V0

This directory contains the V0 (simplified) implementation of the Ballerina Workflow module.

## Structure

```
v0/
├── ballerina_workflow/          # Core workflow module
│   ├── Ballerina.toml          # Module configuration
│   ├── Module.md               # Module documentation
│   └── workflow.bal            # Public API implementation
│
├── examples/                    # Example projects
│   └── simpleuser/             # Simple approval workflow example
│       ├── Ballerina.toml      # Example project configuration
│       └── main.bal            # Example usage code
│
└── native/                      # Java native implementation
    ├── pom.xml                 # Maven build configuration
    └── src/main/java/io/ballerina/stdlib/workflow/v0/
        ├── TemporalClientNative.java           # Temporal client initialization
        ├── client/
        │   └── WorkflowClientNative.java       # Client operations (start, signal, query)
        ├── worker/
        │   └── WorkflowWorkerNative.java       # Worker management
        ├── context/
        │   └── ContextNative.java              # Workflow context operations
        └── utils/
            └── FunctionUtils.java              # Utility functions
```

## Key Differences from V1

The V0 API is intentionally simplified compared to V1:

| Feature | V0 | V1 |
|---------|----|----|
| **Architecture** | Single `Engine` class | Separate `Listener` + `PersistenceProvider` |
| **Workflow Registration** | `engine.register(name, Service)` | `listener.attach(service)` |
| **Client Creation** | `engine.getClient()` | `new Client(provider, workflowType)` |
| **Workflow IDs** | Direct UUID-based IDs | Correlation-based IDs |
| **Listener Pattern** | No listener concept | Full listener lifecycle |
| **Configuration** | Single `TemporalConfig` | Separate config types |

## Building

### Build the Native Library

```bash
cd v0/native
mvn clean package
```

This creates `target/workflow-v0-native-1.0.0.jar`.

### Build the Workflow Module

```bash
cd v0/ballerina_workflow
bal build
```

### Build and Run the Example

```bash
cd v0/examples/simpleuser
bal build
bal run
```

## Module API

### Engine Class

```ballerina
workflow_v0:Engine engine = check new ({
    serviceUrl: "localhost:7233",
    namespace: "default",
    taskQueue: "my-queue"
});

check engine.register("MyWorkflow", MyWorkflowService);
workflow_v0:GenericClient client = check engine.getClient();
check engine.startListen(); // Blocking call
```

### GenericClient

```ballerina
workflow_v0:WorkflowData data = check client->startWorkflow("MyWorkflow", arg1, arg2);
check client->sendSignal("MyWorkflow", data.workflowId, "signalName", param1);
anydata result = check client->query("MyWorkflow", data.workflowId, "queryName");
```

### WFContext (within workflow execution)

```ballerina
string result = check ctx->callActivity(self.myActivity, string, "param1");
check ctx->sleep({hours: 2});
string signal = check ctx->awaitSignal("mySignal", string, 300);
```

## Implementation Status

✅ **Complete:**
- Module structure and organization
- Ballerina API design
- Native Java scaffolding
- Example code

⚠️ **TODO (Native Implementation):**
The Java native classes currently contain placeholders marked with `// TODO:` comments. 
For a working implementation, you need to:

1. **WorkflowWorkerNative**: Implement Temporal Worker creation and service registration
2. **WorkflowClientNative**: Implement workflow start, signal, and query operations
3. **ContextNative**: Implement activity execution, signal awaiting, and sleep
4. **FunctionUtils**: Implement function name extraction from Ballerina function references

Refer to the V1 implementation in `../native/` for guidance on completing these implementations.

## Usage Example

See [examples/simpleuser/main.bal](examples/simpleuser/main.bal) for a complete example demonstrating:
- Workflow service definition
- Activity execution
- Durable sleep
- Signal handling
- Engine configuration and lifecycle

## Notes

- This V0 API is designed for simplicity and ease of understanding
- No correlation-based workflow IDs - uses simple UUID-based IDs
- No separate listener pattern - everything through the Engine class
- Native implementation is simplified with direct method calls
- Suitable for demonstrations and learning the workflow concepts
