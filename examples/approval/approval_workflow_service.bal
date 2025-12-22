import hasitha/workflow;

// Document Approval Workflow Service
service "ApprovalWorkflow" on approvalListener {

    isolated remote function execute(workflow:Context ctx, string requestId, decimal amount, string requester) returns string|error {

        // Step 1: Validate document
        anydata|error data = ctx->callActivity("validateDocument", requestId);
        if data is error {
            return data;
        }

        // Step 2: Wait for either approval or rejection signal
        map<anydata> result = check ctx->awaitSignal(
            "approved",
            86400 // 24 hours timeout
        );

        string signalName = result["signalName"].toString();
        anydata signalData = result["data"] ?: {};
        return "Done";
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
