import ballerina/http;

import hasitha/workflow;

// HTTP service for triggering workflows
// This service provides REST endpoints to:
// - Start workflows
// - Send signals to workflows
// - Query workflow status

// Shared workflow client
final workflow:PersistenceProvider clientProvider = check new ({
    serviceUrl: "localhost:7233",
    namespace: "default"
});
final workflow:Client workflowClient = check new (clientProvider);

// HTTP service on port 9090
service /workflows on new http:Listener(9090) {

    // Send approval signal
    // POST /workflows/approval/signal
    // Body: {"requestId": "REQ-001", "signalName": "approve", "comment": "Approved"}
    resource function post approval/signal(http:Request req) returns json|error {
        json payload = check req.getJsonPayload();
        map<json> data = check payload.ensureType();

        string requestId = check data.requestId.ensureType();
        string signalName = check data.signalName.ensureType();
        string comment = check data.comment.ensureType();

        map<string> correlationData = {
            "workflowType": "ApprovalWorkflow",
            "requestId": requestId
        };

        check workflowClient->signal(
            correlationData,
            signalName,
            {"comment": comment}
        );

        return {
            "status": "success",
            "message": string `${signalName} signal sent to workflow`
        };
    }

    // Start an approval workflow
    // POST /workflows/approval
    // Body: {"requestId": "REQ-001", "amount": 5000.00, "requester": "user@example.com"}
    resource function post approval(http:Request req) returns json|error {

        json payload = check req.getJsonPayload();
        map<json> data = check payload.ensureType();

        string requestId = check data.requestId.ensureType();
        decimal amount = check data.amount.ensureType();
        string requester = check data.requester.ensureType();

        map<string> correlationData = {
            "requestId": requestId
        };

        string workflowId = check workflowClient->startWorkflow(
            "ApprovalWorkflow",
            {
            correlationData: correlationData,
            workflowArgs: [requestId, amount, requester],
            executionTimeout: 7200,
            taskQueue: "approval-processing"
        }
        );

        return {
            "status": "success",
            "workflowId": workflowId,
            "requestId": requestId,
            "message": "Approval workflow started"
        };
    }

    // Query workflow status
    // GET /workflows/approval/status/{requestId}
    resource function get approval/status/[string requestId]() returns json|error {
        map<string> correlationData = {
            "workflowType": "ApprovalWorkflow",
            "requestId": requestId
        };

        anydata result = check workflowClient->query(correlationData, "getStatus");
        
        return check result.cloneWithType();
    }

    // Query workflow metadata
    // GET /workflows/approval/metadata/{requestId}
    resource function get approval/metadata/[string requestId]() returns json|error {
        map<string> correlationData = {
            "workflowType": "ApprovalWorkflow",
            "requestId": requestId
        };

        anydata result = check workflowClient->query(correlationData, "getMetadata");
        
        return check result.cloneWithType();
    }

    // Health check
    resource function get health() returns string {
        return "Workflow HTTP service is running";
    }
}
