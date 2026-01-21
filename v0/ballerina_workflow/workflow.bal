// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# Module initialization - captures runtime environment
function init() = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative"
} external;

# Marks a remote function as a workflow signal handler
public annotation Signal on function;

# Marks a remote function as a workflow query handler  
public annotation Query on function;

# Marks a function as a workflow activity (optional - for documentation)
public annotation Activity on function;

# Workflow service type - all workflow implementations must implement this
public type WorkflowService distinct service object {
};

# Workflow metadata returned when starting a workflow
#
# + workflowId - Unique workflow instance ID
# + workflowName - Workflow type name
public type WorkflowData record {|
    string workflowId;
    string workflowName;
|};

# Temporal configuration for workflow persistence
#
# + serviceUrl - Temporal server URL (default: localhost:7233)
# + namespace - Temporal namespace (default: default)
# + taskQueue - Task queue name for this workflow engine
# + connectionTimeout - Connection timeout in seconds
# + identity - Optional worker identity
public type TemporalConfig record {|
    string serviceUrl = "localhost:7233";
    string namespace = "default";
    string taskQueue = "default";
    int connectionTimeout = 10;
    string identity?;
|};

# Workflow execution engine
# Manages workflow registration, execution, and lifecycle
public class Engine {
    
    private handle nativeWorker;
    private handle nativeClient;
    private TemporalConfig config;

    # Initialize workflow engine with configuration
    #
    # + config - Temporal configuration (optional, uses defaults if not provided)
    # + return - Error if initialization fails
    public function init(TemporalConfig config = {}) returns error? {
        self.config = config.clone();
        handle temporalClient = check initTemporalClient(config);
        self.nativeClient = temporalClient;
        self.nativeWorker = check initWorkflowWorker(temporalClient, config);
    }

    # Obtain a generic client for workflow operations
    #
    # + return - GenericClient or error
    public function getClient() returns GenericClient|error {
        return new GenericClient(self.nativeClient, self.config.taskQueue);
    }

    # Register a workflow implementation with the engine
    #
    # + workflowName - The logical name the engine uses to look up the workflow
    # + svc - The service class typedesc that implements the workflow
    # + return - Error if registration fails
    public function register(string workflowName, typedesc<WorkflowService> svc) returns error? {
        check attachServiceNative(self.nativeWorker, workflowName, svc);
    }

    # Start the engine event loop and begin accepting incoming workflow starts
    # This is a blocking call
    #
    # + return - Error if start fails
    public function startListen() returns error? {
        check startWorkerNative(self.nativeWorker);
    }
    
    # Gracefully stop the workflow engine
    #
    # + return - Error if stop fails
    public function stop() returns error? {
        check stopWorkerNative(self.nativeWorker);
    }
}

# Generic workflow client for starting workflows and sending signals
public client class GenericClient {

    private handle nativeClient;
    private string taskQueue;

    # Initialize generic client (internal use)
    #
    # + nativeClient - Native client handle
    # + taskQueue - Task queue name
    function init(handle nativeClient, string taskQueue) {
        self.nativeClient = nativeClient;
        self.taskQueue = taskQueue;
    }

    # Start a workflow with the given name and parameters
    #
    # + workflowName - Workflow type name
    # + params - Workflow input parameters
    # + return - WorkflowData containing workflow ID or error
    remote function startWorkflow(string workflowName, anydata... params) returns WorkflowData|error {
        string workflowId;
        lock {
            // Use an empty string to let the Java side generate a UUID
            workflowId = check startWorkflowSimpleNative(self.nativeClient, workflowName, "", self.taskQueue, ...params);
        }
        return {workflowId, workflowName};
    }

    # Send a signal to a running workflow
    #
    # + workflowName - Workflow type name (unused, kept for API consistency)
    # + workflowId - Workflow instance ID
    # + signalName - Signal name
    # + params - Signal parameters
    # + return - Error if signal fails
    remote function sendSignal(string workflowName, string workflowId, string signalName, anydata... params) returns error? {
        lock {
            check sendSignalSimpleNative(self.nativeClient, workflowId, signalName, ...params);
        }
    }

    # Query workflow state
    #
    # + workflowName - Workflow type name (unused, kept for API consistency)
    # + workflowId - Workflow instance ID
    # + queryName - Query name
    # + return - Query result or error
    remote function query(string workflowName, string workflowId, string queryName) returns anydata|error {
        final anydata|error result;
        lock {
            result = queryWorkflowSimpleNative(self.nativeClient, workflowId, queryName);
        }
        return result;
    }
}

# Workflow execution context providing workflow APIs
# Automatically injected as first parameter of execute() method
public client class WFContext {

    # Initialize context (internal - called by runtime)
    function init() {
        // No initialization needed - context methods use Temporal SDK directly
    }

    # Await an external signal sent to this workflow instance
    #
    # + signalName - Signal name to wait for
    # + timeoutSeconds - Timeout in seconds (default: 0 = no timeout)
    # + return - Signal data or error on timeout
    isolated remote function awaitSignal(string signalName, int timeoutSeconds = 0) returns anydata|error {
        return awaitSignalNative(signalName, timeoutSeconds);
    }

    # Request execution of an activity function
    #
    # + activityFunc - Activity function reference
    # + args - Activity arguments
    # + return - Activity result or error
    isolated remote function callActivity(function activityFunc, anydata... args) returns anydata|error {
        string activityName = extractFunctionName(activityFunc);
        return executeActivityWithFunctionNative(activityName, activityFunc, ...args);
    }

    # Sleep or delay the workflow
    #
    # + duration - Duration record with day, hours, minutes, seconds
    # + return - Error if sleep fails
    isolated remote function sleep(record {|int day?; int hours?; int minutes?; int seconds?;|} duration) returns error? {
        int totalSeconds = 0;
        if duration.day is int {
            totalSeconds += (<int>duration.day) * 86400;
        }
        if duration.hours is int {
            totalSeconds += (<int>duration.hours) * 3600;
        }
        if duration.minutes is int {
            totalSeconds += (<int>duration.minutes) * 60;
        }
        if duration.seconds is int {
            totalSeconds += (<int>duration.seconds);
        }
        
        lock {
            check sleepNative(totalSeconds);
        }
    }
}

// ============= Native function declarations =============

# Initialize Temporal client
isolated function initTemporalClient(TemporalConfig config) returns handle|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.TemporalClientNative",
    name: "initClient"
} external;

# Initialize workflow worker
isolated function initWorkflowWorker(handle temporalClient, TemporalConfig config) returns handle|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative",
    name: "initWorker"
} external;

# Attach service to worker
isolated function attachServiceNative(handle 'worker, string workflowName, typedesc<WorkflowService> svc) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative",
    name: "attachService"
} external;

# Start workflow worker
isolated function startWorkerNative(handle 'worker) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative",
    name: "startWorker"
} external;

# Stop workflow worker
isolated function stopWorkerNative(handle 'worker) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.worker.WorkflowWorkerNative",
    name: "stopWorker"
} external;

# Start workflow (simplified version)
isolated function startWorkflowSimpleNative(handle 'client, string workflowType, string workflowId, string taskQueue, anydata... args) returns string|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.client.WorkflowClientNative",
    name: "startWorkflowSimple"
} external;

# Send signal (simplified version)
isolated function sendSignalSimpleNative(handle 'client, string workflowId, string signalName, anydata... args) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.client.WorkflowClientNative",
    name: "sendSignalSimple"
} external;

# Query workflow (simplified version)
isolated function queryWorkflowSimpleNative(handle 'client, string workflowId, string queryName, anydata... args) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.client.WorkflowClientNative",
    name: "queryWorkflowSimple"
} external;

# Execute activity
isolated function executeActivityNative(string activityName, anydata... args) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.context.ContextNative",
    name: "executeActivity"
} external;

# Execute activity with function pointer
isolated function executeActivityWithFunctionNative(string activityName, function activityFunc, anydata... args) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.context.ContextNative",
    name: "executeActivityWithFunction"
} external;

# Await signal
isolated function awaitSignalNative(string signalName, int timeoutSeconds) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.context.ContextNative",
    name: "awaitSignal"
} external;

# Sleep workflow
isolated function sleepNative(int seconds) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.context.ContextNative",
    name: "sleep"
} external;

# Extract function name (utility)
isolated function extractFunctionName(function func) returns string = @java:Method {
    'class: "io.ballerina.stdlib.workflow.v0.utils.FunctionUtils",
    name: "extractFunctionName"
} external;
