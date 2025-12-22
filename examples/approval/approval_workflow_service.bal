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

        io:println(string `[Workflow ${self.workflowInstanceId}] Document validated, waiting for signal...`);

        // Step 2: Wait for either approval or rejection signal
        map<anydata> result = check ctx->awaitSignal(
            "approved",
            86400 // 24 hours timeout
        );

        string signalName = result["signalName"].toString();
        anydata signalData = result["data"] ?: {};
        
        io:println(string `[Workflow ${self.workflowInstanceId}] Received signal: ${signalName}`);
        io:println(string `[Workflow ${self.workflowInstanceId}] Total executions in this instance: ${self.executionCount}`);
        
        return string `Workflow ${self.workflowInstanceId} completed by ${self.initiatedBy} after ${self.executionCount} execution(s)`;
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
