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

# Workflow client for starting workflows and sending signals
public isolated client class Client {

    private handle nativeClient;

    # Initialize workflow client with persistence provider
    #
    # + provider - Persistence provider
    # + return - Error if initialization fails
    public isolated function init(PersistenceProvider provider) returns error? {
        log:printDebug("Client.init() called");
        handle temporalClient = provider.getClientHandle();
        self.nativeClient = check initWorkflowClient(temporalClient);
        log:printDebug("Client.init() completed");
    }

    # Start a workflow with correlation data
    #
    # + workflowType - Workflow type name (service name)
    # + params - Workflow start parameters
    # + return - Computed workflow ID or error
    isolated remote function startWorkflow(
            string workflowType,
            WorkflowStartParams params
    ) returns string|error {
        final string|error result;
        lock {
            log:printDebug("StartWorkflow called for workflow type: " + workflowType);
            result = startWorkflowNative(self.nativeClient, workflowType, params.clone());
            log:printDebug("StartWorkflow completed for workflow type: " + workflowType);
        }
        return result;
    }

    # Send signal to workflow using correlation data
    #
    # + correlationData - Correlation data to identify workflow instance
    # + signalName - Signal name
    # + signalData - Signal payload data
    # + return - Error if signal fails
    isolated remote function signal(
            map<string> correlationData,
            string signalName,
            map<string> signalData = {}
    ) returns error? {
        lock {
            log:printDebug("Signal called for signal: " + signalName);
            check sendSignalNative(self.nativeClient, correlationData.clone(), signalName, signalData.clone());
            log:printDebug("Signal completed for signal: " + signalName);
        }
    }

    # Query workflow state
    #
    # + correlationData - Correlation data to identify workflow instance
    # + queryName - Query name
    # + return - Query result or error
    isolated remote function query(
            map<string> correlationData,
            string queryName
    ) returns anydata|error {
        final anydata|error result;
        lock {
            log:printDebug("Query called for query: " + queryName);
            result = queryWorkflowNative(self.nativeClient, correlationData.clone(), queryName);
            log:printDebug("Query completed for query: " + queryName);
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
        map<string> correlationData,
        string signalName,
        map<string> signalData
) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "sendSignal"
} external;

isolated function queryWorkflowNative(
        handle 'client,
        map<string> correlationData,
        string queryName
) returns anydata|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.client.WorkflowClientNative",
    name: "queryWorkflow"
} external;
