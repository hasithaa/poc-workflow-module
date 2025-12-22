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

# Workflow client for starting workflows and sending signals
public isolated client class Client {

    private handle nativeClient;
    private final string workflowType;

    # Initialize workflow client with persistence provider and workflow type
    #
    # + provider - Persistence provider
    # + workflowType - Workflow type name (service name) for all operations
    # + return - Error if initialization fails
    public isolated function init(PersistenceProvider provider, string workflowType) returns error? {
        // io:println("[BClient] Client.init() called for workflow type: " + workflowType);
        handle temporalClient = provider.getClientHandle();
        self.nativeClient = check initWorkflowClient(temporalClient);
        self.workflowType = workflowType;
        // io:println("[BClient] Client.init() completed");
    }

    # Start a workflow with correlation data
    #
    # + params - Workflow start parameters
    # + return - Computed workflow ID or error
    isolated remote function startWorkflow(
            WorkflowStartParams params
    ) returns string|error {
        final string|error result;
        lock {
            // io:println("[BClient] StartWorkflow called for workflow type: " + self.workflowType);
            result = startWorkflowNative(self.nativeClient, self.workflowType, params.clone());
            // io:println("[BClient] StartWorkflow completed for workflow type: " + self.workflowType);
        }
        return result;
    }

    # Send signal to workflow using correlation data
    #
    # + correlationData - Correlation data to identify workflow instance (without workflowType)
    # + signalName - Signal name
    # + signalData - Signal payload data
    # + return - Error if signal fails
    isolated remote function signal(
            map<string> correlationData,
            string signalName,
            map<string> signalData = {}
    ) returns error? {
        lock {
            // io:println("[BClient] Signal called for signal: " + signalName);
            check sendSignalNative(self.nativeClient, self.workflowType, correlationData.clone(), signalName, signalData.clone());
            // io:println("[BClient] Signal completed for signal: " + signalName);
        }
    }

    # Query workflow state
    #
    # + correlationData - Correlation data to identify workflow instance (without workflowType)
    # + queryName - Query name
    # + return - Query result or error
    isolated remote function query(
            map<string> correlationData,
            string queryName
    ) returns anydata|error {
        final anydata|error result;
        lock {
            // io:println("[BClient] Query called for query: " + queryName);
            result = queryWorkflowNative(self.nativeClient, self.workflowType, correlationData.clone(), queryName);
            // io:println("[BClient] Query completed for query: " + queryName);
        }
        return result;
    }
}

isolated function initWorkflowClient(handle temporalClient) returns handle|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "initClient"
} external;

isolated function startWorkflowNative(
        handle 'client,
        string workflowType,
        WorkflowStartParams params
) returns string|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "startWorkflow"
} external;

isolated function sendSignalNative(
        handle 'client,
        string workflowType,
        map<string> correlationData,
        string signalName,
        map<string> signalData
) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "sendSignal"
} external;

isolated function queryWorkflowNative(
        handle 'client,
        string workflowType,
        map<string> correlationData,
        string queryName
) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "queryWorkflow"
} external;
