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

import hasitha/workflow_v0;
import ballerina/io;
import ballerina/lang.runtime;

# Example workflow service demonstrating approval workflow pattern
service class ApprovalService {
    *workflow_v0:WorkflowService;

    # Workflow execution method
    # The workflow engine calls this to start a new workflow instance
    #
    # + ctx - Workflow context providing workflow APIs
    # + startArg1 - First workflow input parameter
    # + startArg2 - Second workflow input parameter
    # + return - Workflow result or error
    isolated remote function execute(workflow_v0:WFContext ctx, string startArg1, int startArg2) returns string|error {

        // Activity call - pass the function reference, expected return type, and arguments
        anydata emailResult = check ctx->callActivity(self.sendEmail, "user@example.com", "Subject", "Body");

        // Sleep for 2 days - workflow will durably wait
        check ctx->sleep({seconds: 10});

        // Await an external signal named `submitApproval` and receive a `string`
        // The timeout is set to 300 seconds (5 minutes)
        anydata approvalData = check ctx->awaitSignal("submitApproval", 300);
        
        return "Done:" + approvalData.toString();
    }

    # Activity function for sending email
    # This can be any function which accepts parameters/returns anydata
    # Activities are invoked by the workflow runtime via `WFContext->callActivity`
    #
    # + to - Email recipient
    # + subject - Email subject
    # + body - Email body
    # + return - Success message or error
    isolated function sendEmail(string to, string subject, string body) returns string|error {
        // Simulate sending an email
        // In real implementation, this would call an SMTP server or email service
        return "Email sent to: " + to;
    }

    # Signal handler for approval submission
    # This acts as an event handler for when an external party submits approval data
    # This is invoked via the client interface
    # Returned data is passed back to the workflow via `awaitSignal`
    #
    # + arg1 - First signal parameter
    # + arg2 - Second signal parameter
    # + return - Signal response data or error
    @workflow_v0:Signal
    remote isolated function submitApproval(string arg1, int arg2) returns string|error {
        return "Approved: " + arg1;
    }
}

# Main function demonstrating workflow usage
public function main() returns error? {

    // Create engine with Temporal configuration
    workflow_v0:Engine engine = check new ({
        serviceUrl: "localhost:7233",
        namespace: "default",
        taskQueue: "approval-queue"
    });

    // Register the workflow service with a logical name
    check engine.register("MyApprovalService", ApprovalService);

    // Start the worker to process workflows (this would block, so in real apps run in separate process)
    // For this example, we'll just start it and let it run
    io:println("Starting workflow worker...");
    check engine.startListen();
    
    // Get a generic client for workflow operations
    workflow_v0:GenericClient cl = check engine.getClient();

    // Start a workflow instance with input parameters
    workflow_v0:WorkflowData data = check cl->startWorkflow("MyApprovalService", "arg1", 2);
    io:println("Started workflow: ", data);

    // Wait a bit for the workflow to start executing
    runtime:sleep(2);
    
    // Send an approval signal to the running workflow
    io:println("Sending approval signal...");
    check cl->sendSignal(data.workflowId, "submitApproval", "approvalData", 5);
    io:println("Signal sent successfully");
    
    // Start listening for workflow tasks (blocking call)
    // This will process the workflow execution
    io:println("Worker listening on task queue: approval-queue");
}
