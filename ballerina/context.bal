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
import ballerina/io;

# Module initialization - captures runtime environment
function init() = @java:Method {
    'class: "io.ballerina.stdlib.workflow.worker.WorkflowWorkerNative"
} external;

# Workflow execution context providing workflow APIs
# Automatically injected as first parameter of execute() method
public isolated client class Context {

    private handle nativeContext;

    # Initialize context (internal - called by runtime)
    isolated function init(handle nativeContext) {
        io:println("[BContext] Context init called");
        self.nativeContext = nativeContext;
    }

    # Execute activity with replay protection
    #
    # + activityName - Activity function name
    # + args - Activity arguments
    # + return - Activity result or error
    isolated remote function callActivity(
            string activityName,
            anydata... args
    ) returns anydata|error {
        final anydata|error result;
        lock {
            io:println("[BContext] CallActivity called for activity: " + activityName);
            result = executeActivityNative(self.nativeContext, activityName, args.clone());
            io:println("[BContext] CallActivity completed for activity: " + activityName);
        }
        return result;
    }

    # Wait for signal by name
    #
    # + signalName - Signal name to wait for
    # + timeoutSeconds - Timeout in seconds
    # + return - Signal data or error on timeout
    isolated remote function awaitSignal(
            string signalName,
            int timeoutSeconds
    ) returns map<string>|error {
        final map<string>|error result;
        lock {
            io:println("[BContext] AwaitSignal called for signal: " + signalName);
            result = awaitSignalNative(self.nativeContext, signalName, timeoutSeconds);
            io:println("[BContext] AwaitSignal completed for signal: " + signalName);
        }
        return result;
    }

    # Wait for boolean condition
    #
    # + timeoutSeconds - Timeout in seconds
    # + condition - Condition function returning boolean
    # + return - True if condition met, false on timeout, error on failure
    isolated remote function awaitCondition(
            int timeoutSeconds,
            function () returns boolean condition
    ) returns boolean|error {
        final boolean|error result;
        lock {
            io:println("[BContext] AwaitCondition called");
            result = awaitConditionNative(self.nativeContext, timeoutSeconds, condition);
            io:println("[BContext] AwaitCondition completed");
        }
        return result;
    }

    # Wait for any of multiple signals
    #
    # + signalNames - Array of signal names
    # + timeoutSeconds - Timeout in seconds
    # + return - Signal result or error on timeout
    isolated remote function awaitAnySignal(
            string[] signalNames,
            int timeoutSeconds
    ) returns SignalResult|error {
        final SignalResult|error result;
        lock {
            io:println("[BContext] AwaitAnySignal called");
            result = awaitAnySignalNative(self.nativeContext, signalNames.clone(), timeoutSeconds);
            io:println("[BContext] AwaitAnySignal completed");
        }
        return result;
    }

    # Durable sleep - suspends workflow
    #
    # + seconds - Sleep duration in seconds
    # + return - Error if sleep fails
    isolated remote function sleep(int seconds) returns error? {
        lock {
            io:println("[BContext] Sleep called for " + seconds.toString() + " seconds");
            check sleepNative(self.nativeContext, seconds);
            io:println("[BContext] Sleep completed");
        }
    }

    # Get correlation ID for this workflow instance
    #
    # + return - Correlation ID string
    isolated remote function getCorrelationId() returns string {
        final string result;
        lock {
            io:println("[BContext] GetCorrelationId called");
            result = getCorrelationIdNative(self.nativeContext);
            io:println("[BContext] GetCorrelationId completed: " + result);
        }
        return result;
    }

    # Check if workflow is currently replaying
    #
    # + return - True if replaying, false otherwise
    isolated remote function isReplaying() returns boolean {
        final boolean result;
        lock {
            io:println("[BContext] IsReplaying called");
            result = isReplayingNative(self.nativeContext);
            io:println("[BContext] IsReplaying completed: " + result.toString());
        }
        return result;
    }
}

isolated function executeActivityNative(
        handle context,
        string activityName,
        anydata[] args
) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "executeActivity"
} external;

isolated function awaitSignalNative(
        handle context,
        string signalName,
        int timeoutSeconds
) returns map<string>|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "awaitSignal"
} external;

isolated function awaitConditionNative(
        handle context,
        int timeoutSeconds,
        function () returns boolean condition
) returns boolean|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "awaitCondition"
} external;

isolated function awaitAnySignalNative(
        handle context,
        string[] signalNames,
        int timeoutSeconds
) returns SignalResult|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "awaitAnySignal"
} external;

isolated function sleepNative(handle context, int seconds) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "sleep"
} external;

isolated function getCorrelationIdNative(handle context) returns string = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "getCorrelationId"
} external;

isolated function isReplayingNative(handle context) returns boolean = @java:Method {
    'class: "io.ballerina.stdlib.workflow.context.WorkflowContextNative",
    name: "isReplaying"
} external;
