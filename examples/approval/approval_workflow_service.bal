import hasitha/workflow;
import ballerina/io;

// Document Approval Workflow Service
// IMPORTANT: Each workflow instance gets its own copy of this service object
// This demonstrates per-instance state isolation introduced in Session 2
service "ApprovalWorkflow" on approvalListener {

    // Service-level state - unique per workflow instance
    private string workflowInstanceId = "";
    private string initiatedBy = "";
    private int executionCount = 0;
    private string status = "";

    isolated remote function execute(workflow:Context ctx, string requestId, decimal amount, string requester) returns string|error {

        // Store workflow metadata in service-level state
        // Each workflow instance has its own copy of these fields
        self.workflowInstanceId = requestId;
        self.initiatedBy = requester;
        self.executionCount += 1;

        io:println(string `[Workflow ${self.workflowInstanceId}] Execution #${self.executionCount} initiated by ${self.initiatedBy}`);
        io:println(string `[Workflow ${self.workflowInstanceId}] Processing approval for amount: ${amount}`);

        // Step 1: Validate document
        anydata|error data = ctx->callActivity("validateDocument", requestId);
        if data is error {
            io:println(string `[Workflow ${self.workflowInstanceId}] Validation failed: ${data.message()}`);
            return data;
        }

        self.status = "Document validated";

        io:println(string `[Workflow ${self.workflowInstanceId}] Document validated, waiting for signal...`);

        // Step 2: Wait for approval signal
        // Now returns anydata directly from signal handler remote method if defined
        anydata result = check ctx->awaitSignal(
            "approved",
            86400 // 24 hours timeout
        );
        
        io:println(string `[Workflow ${self.workflowInstanceId}] Received signal result: ${result.toString()}`);
        io:println(string `[Workflow ${self.workflowInstanceId}] Total executions in this instance: ${self.executionCount}`);
        
        self.status = "Done";
        return string `Workflow ${self.workflowInstanceId} completed by ${self.initiatedBy} after ${self.executionCount} execution(s)`;
    }

    # Signal handler for 'approved' signal
    # This remote method is called when 'approved' signal is received
    # Allows extending signal logic with custom processing
    # + signalData - Data sent with the signal
    # + return - Result to be returned to awaitSignal caller
    isolated remote function approved(map<anydata> signalData) returns anydata {
        io:println(string `[Workflow ${self.workflowInstanceId}] Signal handler 'approved' called`);
        
        // Extract comment from signal data
        string comment = signalData["comment"].toString();
        io:println(string `[Workflow ${self.workflowInstanceId}] Approval comment: ${comment}`);
        
        // Can perform additional processing here
        // Update state, log, etc.
        self.status = "Approved";
        
        // Return enriched data back to awaitSignal caller
        return {
            "approved": true,
            "comment": comment,
            "processedBy": "signal-handler",
            "timestamp": "2025-12-22T10:00:00Z"
        };
    }

    # Query workflow status - read-only operation
    # Returns current workflow state without modifying it
    isolated remote function getStatus() returns map<anydata> {
        io:println(string `[Workflow ${self.workflowInstanceId}] Query: getStatus() called`);
        
        return {
            "workflowInstanceId": self.workflowInstanceId,
            "initiatedBy": self.initiatedBy,
            "executionCount": self.executionCount,
            "status": self.status
        };
    }

    # Query workflow metadata
    isolated remote function getMetadata() returns map<anydata> {
        io:println(string `[Workflow ${self.workflowInstanceId}] Query: getMetadata() called`);
        
        return {
            "id": self.workflowInstanceId,
            "requester": self.initiatedBy,
            "executions": self.executionCount
        };
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
