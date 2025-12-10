import ballerina/io;
import hasitha/workflow;

// Document Approval Workflow Service
service "ApprovalWorkflow" on approvalListener {

    isolated remote function execute(workflow:Context ctx, string requestId, decimal amount, string requester) returns string|error {
        io:println(string `Starting approval workflow for request: ${requestId}, amount: ${amount}, requester: ${requester}`);

        // Step 1: Validate document
        anydata _ = check ctx->callActivity("validateDocument", requestId);

        // Step 2: Wait for either approval or rejection signal
        workflow:SignalResult result = check ctx->awaitAnySignal(
            ["approved", "rejected", "needsRevision"],
            86400 // 24 hours timeout
        );

        string signalName = result.signalName;
        map<string> signalData = result.data;

        if signalName == "approved" {
            string? approverValue = signalData["approver"];
            string approver = approverValue is string ? approverValue : "unknown";
            io:println(string `Document approved by: ${approver}`);
            
            anydata _ = check ctx->callActivity("publishDocument", requestId);
            anydata _ = check ctx->callActivity("notifySubmitter", requester, "Your document has been approved");
            
            return "Document approved and published";
            
        } else if signalName == "rejected" {
            string? reasonValue = signalData["reason"];
            string reason = reasonValue is string ? reasonValue : "No reason provided";
            io:println(string `Document rejected: ${reason}`);
            
            anydata _ = check ctx->callActivity("notifySubmitter", requester, string `Document rejected: ${reason}`);
            
            return string `Document rejected: ${reason}`;
            
        } else {
            // needsRevision
            string? commentsValue = signalData["comments"];
            string comments = commentsValue is string ? commentsValue : "Revision required";
            io:println(string `Document needs revision: ${comments}`);
            
            anydata _ = check ctx->callActivity("notifySubmitter", requester, string `Revision needed: ${comments}`);
            
            // Wait for resubmission
            map<string> _ = check ctx->awaitSignal("resubmitted", 172800); // 48 hours
            
            // Recursive approval check - in real scenario, might want loop protection
            anydata _ = check ctx->callActivity("notifySubmitter", requester, "Document resubmitted for approval");
            
            return "Document resubmitted";
        }
    }

}

// Initialize the persistence provider and listener for approval workflow
final workflow:PersistenceProvider approvalProvider = createApprovalProvider();
listener workflow:Listener approvalListener = check new (approvalProvider, {taskQueue: "approval-processing"});

function createApprovalProvider() returns workflow:PersistenceProvider {
    workflow:PersistenceProvider provider = checkpanic new ({
        serviceUrl: "localhost:7233",
        namespace: "default"
    });
    return provider;
}