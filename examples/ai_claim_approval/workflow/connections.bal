import ballerina/email;
import hasitha/workflow;

// Shared workflow client for ApprovalWorkflow
final workflow:PersistenceProvider workflowProvider = check new ({
    serviceUrl: "localhost:7233",
    namespace: "default"
});

final email:SmtpClient smtpClient = check new(
    host = "smtp.gmail.com",
    port = 465,
    username = smtpUser,
    password = smtpPassword
);


final workflow:Client workflowClient = check new (workflowProvider, "ClaimApprovalWorkflow");
