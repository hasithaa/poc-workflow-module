import ballerina/log;
import hasitha/workflow;

// Document Approval Workflow Service
service "ApprovalWorkflow" on approvalListener {

    isolated remote function execute(workflow:Context ctx, string requestId, decimal amount, string requester) returns string|error {
        log:printInfo(string `[Workflow] ApprovalWorkflow STARTED - requestId: ${requestId}, amount: ${amount}, requester: ${requester}`);

        // Step 1: Validate document
        log:printInfo(string `[Workflow] Step 1: Validating document for requestId: ${requestId}`);
        anydata|error data = ctx->callActivity("validateDocument", requestId);
        if data is error {
            log:printError(string `[Workflow] Document validation FAILED for requestId: ${requestId}`, 'error = data);
            return data;
        }
        log:printInfo(string `[Workflow] Document validation SUCCEEDED for requestId: ${requestId}`);

        // Step 2: Wait for either approval or rejection signal
        log:printInfo(string `[Workflow] Step 2: Awaiting signal (approved/rejected/needsRevision) for requestId: ${requestId}`);
        map<anydata> result = check ctx->awaitSignal(
            "approved",
            86400 // 24 hours timeout
        );

        string signalName = result["signalName"].toString();
        anydata signalData = result["data"] ?: {};
        log:printInfo(string `[Workflow] Signal RECEIVED: '${signalName}' for requestId: ${requestId}`);
        log:printDebug(string `[Workflow] Signal data: ${signalData.toString()}`);

        return "Done";
        // if signalName == "approved" {
        //     string? approverValue = signalData["approver"];
        //     string approver = approverValue is string ? approverValue : "unknown";
        //     log:printInfo(string `[Workflow] Processing APPROVED signal - approver: ${approver}, requestId: ${requestId}`);
            
        //     log:printInfo(string `[Workflow] Publishing document for requestId: ${requestId}`);
        //     anydata _ = check ctx->callActivity("publishDocument", requestId);
            
        //     log:printInfo(string `[Workflow] Notifying submitter (${requester}) of approval`);
        //     anydata _ = check ctx->callActivity("notifySubmitter", requester, "Your document has been approved");
            
        //     log:printInfo(string `[Workflow] ApprovalWorkflow COMPLETED SUCCESSFULLY - requestId: ${requestId}, status: APPROVED`);
        //     return "Document approved and published";
            
        // } else if signalName == "rejected" {
        //     string? reasonValue = signalData["reason"];
        //     string reason = reasonValue is string ? reasonValue : "No reason provided";
        //     log:printWarn(string `[Workflow] Processing REJECTED signal - reason: ${reason}, requestId: ${requestId}`);
            
        //     log:printInfo(string `[Workflow] Notifying submitter (${requester}) of rejection`);
        //     anydata _ = check ctx->callActivity("notifySubmitter", requester, string `Document rejected: ${reason}`);
            
        //     log:printInfo(string `[Workflow] ApprovalWorkflow COMPLETED - requestId: ${requestId}, status: REJECTED`);
        //     return string `Document rejected: ${reason}`;
            
        // } else {
        //     // needsRevision
        //     string? commentsValue = signalData["comments"];
        //     string comments = commentsValue is string ? commentsValue : "Revision required";
        //     log:printWarn(string `[Workflow] Processing NEEDS_REVISION signal - comments: ${comments}, requestId: ${requestId}`);
            
        //     log:printInfo(string `[Workflow] Notifying submitter (${requester}) that revision is needed`);
        //     anydata _ = check ctx->callActivity("notifySubmitter", requester, string `Revision needed: ${comments}`);
            
        //     // Wait for resubmission
        //     log:printInfo(string `[Workflow] Awaiting 'resubmitted' signal for requestId: ${requestId}`);
        //     map<string> _ = check ctx->awaitSignal("resubmitted", 172800); // 48 hours
        //     log:printInfo(string `[Workflow] Resubmission signal received for requestId: ${requestId}`);
            
        //     // Recursive approval check - in real scenario, might want loop protection
        //     log:printInfo(string `[Workflow] Processing resubmitted document for requestId: ${requestId}`);
        //     anydata _ = check ctx->callActivity("notifySubmitter", requester, "Document resubmitted for approval");
            
        //     log:printInfo(string `[Workflow] ApprovalWorkflow COMPLETED - requestId: ${requestId}, status: RESUBMITTED`);
        //     return "Document resubmitted";
        // }
    }

}

// Initialize the persistence provider and listener for approval workflow
final workflow:PersistenceProvider approvalProvider = createApprovalProvider();
listener workflow:Listener approvalListener = check new (approvalProvider, {taskQueue: "approval-processing"});

function createApprovalProvider() returns workflow:PersistenceProvider {
    log:printInfo("[STARTUP] Creating ApprovalWorkflow persistence provider");
    workflow:PersistenceProvider provider = checkpanic new ({
        serviceUrl: "localhost:7233",
        namespace: "default"
    });
    log:printInfo("[STARTUP] ApprovalWorkflow persistence provider created successfully");
    return provider;
}