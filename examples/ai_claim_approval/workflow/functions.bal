import ballerina/io;
import ballerina/ai;

import hasitha/workflow;

@workflow:Activity // Should be Claims[]
function callStatisticalRiskAnalysisAgent(string claims) returns string|error {
    final string instructions = string `You are a JSON-to-Decision logic engine. You must evaluate the data based on these two rules:
Total Sum ≤ 5000.
No single claim > 60% of the total.
CRITICAL INSTRUCTIONS:
Do NOT show your calculations.
Do NOT explain your logic if the result is "Approve".
Do NOT provide any conversational text, pleasantries, or markdown.
If Approved: Your entire response must be exactly one word: Approve
If Rejected: Your response must start with Reject: followed by the reason.
Examples:
Input: [Claims totaling 3000, max claim 1000] -> Output: Approve
Input: [Claims totaling 6000, max claim 2000] -> Output: Reject: Total sum exceeds 5000.`;

    final ai:Agent taskAssistantAgent = check new ({
        systemPrompt: {
            role: "Statistical Risk Validator",
            instructions
        },
        // Specify the functions the agent can use as tools.
        tools: [],
        // Use the default model provider (with configuration added
        // via a Ballerina VS Code command).
        model: check ai:getDefaultModelProvider()
    });
    return taskAssistantAgent.run(string `claims to be reviewed: ${claims}`);
}

@workflow:Activity
isolated function sendManualApprovalRequest(string to, string reqId, string userId) returns error? {

    final string subject = "Document Approved";

    final string body = string `
    <h1>Document Approval Needed</h1>

    <p>Please approve the following claim request.</p>
    <p>Request ID: ${reqId}</p>
    <p>User ID: ${userId}</p>
    <p>Click <a href="http://localhost:9090/document/review?reqId=${reqId}&userId=${userId}">here</a> to review.</p>
    `;
    io:println("Sending email to " + to + " review request " + reqId);
    check smtpClient->send(to, subject, "hasithatw@gmail.com", body, htmlBody = body, contentType = "text/html");
}

// Tempory code
function init() returns error? {
    check workflow:registerActivity("callStatisticalRiskAnalysisAgent", callStatisticalRiskAnalysisAgent);
    check workflow:registerActivity("sendManualApprovalRequest", sendManualApprovalRequest);
}
