import ballerina/http;
import hasitha/workflow;
import ballerina/log;

// Register activities function - called at startup
// Note: Activity implementations are defined in respective workflow files
function registerActivities() returns error? {
    log:printInfo("[STARTUP] Registering activity implementations...");
    
    // Approval workflow activities (defined in approval_workflow.bal)
    check workflow:registerActivity("validateDocument", validateDocument);
    log:printInfo("[STARTUP] Registered activity: validateDocument");
    
    check workflow:registerActivity("publishDocument", publishDocument);
    log:printInfo("[STARTUP] Registered activity: publishDocument");
    
    check workflow:registerActivity("notifySubmitter", notifySubmitter);
    log:printInfo("[STARTUP] Registered activity: notifySubmitter");
    
    // Order workflow activities (defined in order_workflow.bal)  
    // TODO: Add order workflow activities when implemented
    
    log:printInfo("[STARTUP] All activities registered successfully");
}

// Initialize activities at module load time
function init() {
    log:printInfo("[STARTUP] Initializing approval workflow application...");
    error? result = registerActivities();
    if result is error {
        log:printError("[STARTUP] Failed to register activities", 'error = result);
    } else {
        log:printInfo("[STARTUP] Application initialization completed successfully");
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
    log:printInfo("[STARTUP] Creating workflow client provider for localhost:7233");
    workflow:PersistenceProvider provider = check new ({
        serviceUrl: "localhost:7233",
        namespace: "default"
    });
    log:printInfo("[STARTUP] Workflow client provider created successfully");
    return provider;
}
    
function createWorkflowClient() returns workflow:Client|error {
    log:printInfo("[STARTUP] Creating workflow client");
    workflow:Client wfClient = check new (clientProvider);
    log:printInfo("[STARTUP] Workflow client created successfully");
    return wfClient;
}

// HTTP service on port 9090
service /workflows on new http:Listener(9090) {

    // Send approval signal
    // POST /workflows/approval/signal
    // Body: {"requestId": "REQ-001", "signalName": "approve", "comment": "Approved"}
    resource function post approval/signal(http:Request req) returns json|error {
        log:printInfo("[HTTP] Received signal request");
        
        json payload = check req.getJsonPayload();
        map<json> data = check payload.ensureType();
        
        string requestId = check data.requestId.ensureType();
        string signalName = check data.signalName.ensureType();
        string comment = check data.comment.ensureType();

        log:printInfo(string `[HTTP] Sending signal '${signalName}' to workflow with requestId: ${requestId}`);
        log:printDebug(string `[HTTP] Signal comment: ${comment}`);

        map<string> correlationData = {
            "requestId": requestId
        };

        check workflowClient->signal(
            correlationData,
            signalName,
            {"comment": comment}
        );

        log:printInfo(string `[HTTP] Signal '${signalName}' sent successfully to requestId: ${requestId}`);

        return {
            "status": "success",
            "message": string `${signalName} signal sent to workflow`
        };
    }

    // Start an approval workflow
    // POST /workflows/approval
    // Body: {"requestId": "REQ-001", "amount": 5000.00, "requester": "user@example.com"}
    resource function post approval(http:Request req) returns json|error {
        log:printInfo("[HTTP] Received workflow start request");
        
        json payload = check req.getJsonPayload();
        map<json> data = check payload.ensureType();
        
        string requestId = check data.requestId.ensureType();
        decimal amount = check data.amount.ensureType();
        string requester = check data.requester.ensureType();

        log:printInfo(string `[HTTP] Starting ApprovalWorkflow for requestId: ${requestId}`);
        log:printDebug(string `[HTTP] Workflow params - Amount: ${amount}, Requester: ${requester}`);

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

        log:printInfo(string `[HTTP] ApprovalWorkflow started successfully - WorkflowId: ${workflowId}, RequestId: ${requestId}`);

        return {
            "status": "success",
            "workflowId": workflowId,
            "requestId": requestId,
            "message": "Approval workflow started"
        };
    }

    // Health check
    resource function get health() returns string {
        log:printDebug("[HTTP] Health check endpoint called");
        return "Workflow HTTP service is running";
    }
}