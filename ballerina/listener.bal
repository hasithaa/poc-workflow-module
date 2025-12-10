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
import ballerina/log;

# Workflow listener for registering workflow services
public class Listener {

    private handle nativeWorker;
    private ListenerConfig config;

    # Initialize workflow listener
    #
    # + provider - Persistence provider
    # + config - Listener configuration
    # + return - Error if initialization fails
    public isolated function init(
            PersistenceProvider provider,
            ListenerConfig config
    ) returns error? {
        log:printDebug("Listener.init() called for task queue: " + config.taskQueue);
        self.config = config.clone();
        handle temporalClient = provider.getClientHandle();
        self.nativeWorker = check initWorkflowWorker(temporalClient, config);
        log:printDebug("Listener.init() completed for task queue: " + config.taskQueue);
    }

    # Attach workflow service to listener
    #
    # + s - Workflow service object
    # + name - Service name (workflow type)
    # + return - Error if attachment fails
    public function attach(service object {} s, string[]|string? name = ()) returns error? {
        string serviceName = name is string ? name : (name is string[] ? name[0] : "");
        log:printDebug("Listener.attach() called for service: " + serviceName);
        check attachServiceNative(self.nativeWorker, s, serviceName);
        log:printDebug("Listener.attach() completed for service: " + serviceName);
    }

    # Detach workflow service from listener
    #
    # + s - Workflow service object
    # + return - Error if detachment fails
    public function detach(service object {} s) returns error? {
        log:printDebug("Listener.detach() called");
        check detachServiceNative(self.nativeWorker, s);
        log:printDebug("Listener.detach() completed");
    }

    # Start the workflow worker (blocking)
    #
    # + return - Error if start fails
    public function 'start() returns error? {
        log:printDebug("Listener.start() called for task queue: " + self.config.taskQueue);
        check startWorkerNative(self.nativeWorker);
        log:printDebug("Listener.start() completed for task queue: " + self.config.taskQueue);
    }

    # Gracefully stop the workflow worker
    #
    # + return - Error if stop fails
    public function gracefulStop() returns error? {
        log:printDebug("Listener.gracefulStop() called");
        check stopWorkerNative(self.nativeWorker);
        log:printDebug("Listener.gracefulStop() completed");
    }

    # Immediately stop the workflow worker
    #
    # + return - Error if stop fails
    public function immediateStop() returns error? {
        log:printDebug("Listener.immediateStop() called");
        check stopWorkerNative(self.nativeWorker);
        log:printDebug("Listener.immediateStop() completed");
    }
}

isolated function initWorkflowWorker(
        handle temporalClient,
        ListenerConfig config
) returns handle|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "initWorker"
} external;

isolated function attachServiceNative(
        handle 'worker,
        service object {} serviceObj,
        string serviceName
) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "attachService"
} external;

isolated function detachServiceNative(
        handle 'worker,
        service object {} serviceObj
) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "detachService"
} external;

isolated function startWorkerNative(handle 'worker) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "startWorker"
} external;

isolated function stopWorkerNative(handle 'worker) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "stopWorker"
} external;

# Register an activity implementation function
#
# + activityName - Name of the activity (must match what workflows call)
# + activityFunction - Function pointer to the activity implementation
# + return - Error if registration fails
public isolated function registerActivity(
        string activityName,
        function activityFunction
) returns error? {
    log:printDebug("RegisterActivity called for activity: " + activityName);
    error? result = registerActivityNative(activityName, activityFunction);
    log:printDebug("RegisterActivity completed for activity: " + activityName);
    return result;
}

isolated function registerActivityNative(
        string activityName,
        function activityFunction
) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative",
    name: "registerActivity"
} external;
