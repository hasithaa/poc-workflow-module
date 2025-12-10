import ballerina/http;
import hasitha/workflow;
import ballerina/io;

// Register activities function - called at startup
// Note: Activity implementations are defined in respective workflow files
function registerActivities() returns error? {
    io:println("[INIT] Registering activity implementations...");
    
    // Approval workflow activities (defined in approval_workflow.bal)
    check workflow:registerActivity("validateDocument", validateDocument);
    check workflow:registerActivity("publishDocument", publishDocument);
    check workflow:registerActivity("notifySubmitter", notifySubmitter);
    
    // Order workflow activities (defined in order_workflow.bal)  
    // TODO: Add order workflow activities when implemented
    
    io:println("[INIT] All activities registered successfully");
}

// Initialize activities at module load time
function init() {
    error? result = registerActivities();
    if result is error {
        io:println(string `[ERROR] Failed to register activities: ${result.message()}`);
    }
}

// HTTP service for triggering workflows
// This service provides REST endpoints to:
// - Start workflows
// - Send signals to workflows
// - Query workflow status

// Shared workflow client
final workflow:PersistenceProvider clientProvider = check createClientProvider();
final workflow:Client workflowClient = check createWorkflowClient();

function createClientProvider() returns workflow:PersistenceProvider|error {
    return check new ({
        serviceUrl: "localhost:7233",
        namespace: "default"
    });
}
    
function createWorkflowClient() returns workflow:Client|error {
    return check new (clientProvider);
}

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
            "requestId": requestId
        };

        io:println("Sending Signal ", correlationData);
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

    // Health check
    resource function get health() returns string {
        return "Workflow HTTP service is running";
    }
}