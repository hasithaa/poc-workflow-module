import hasitha/workflow;

// Shared workflow client for ApprovalWorkflow
final workflow:PersistenceProvider clientProvider = check new ({
    serviceUrl: "localhost:7233",
    namespace: "default"
});
final workflow:Client workflowClient = check new (clientProvider, "ClaimApprovalWorkflow");
