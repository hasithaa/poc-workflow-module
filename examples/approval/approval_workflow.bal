import ballerina/log;
import ballerina/http;

// Document Approval Workflow Example
// This example demonstrates:
// - Waiting for any of multiple signals (approved/rejected/needsRevision)
// - Conditional logic based on received signals
// - Handling multi-path workflows

// Activity implementations for approval workflow
isolated function validateDocument(string documentId) returns error? {
    log:printInfo(string `[Activity] validateDocument START - documentId: ${documentId}`);

    http:Client cl = check new ("http://localhost:9090/workflows");
    string j = check cl->get("/health2");
    log:printDebug(string `[Activity] Validation service response: ${j}`);
    
    log:printInfo(string `[Activity] validateDocument END - documentId: ${documentId}`);
}

isolated function publishDocument(string documentId) returns error? {
    log:printInfo(string `[Activity] publishDocument START - documentId: ${documentId}`);
    // Simulate publishing logic
    log:printInfo(string `[Activity] Document ${documentId} published successfully`);
    log:printInfo(string `[Activity] publishDocument END - documentId: ${documentId}`);
}

isolated function notifySubmitter(string submitter, string message) returns error? {
    log:printInfo(string `[Activity] notifySubmitter START - submitter: ${submitter}`);
    log:printDebug(string `[Activity] Notification message: ${message}`);
    // Simulate notification logic
    log:printInfo(string `[Activity] Notification sent to ${submitter}`);
    log:printInfo(string `[Activity] notifySubmitter END - submitter: ${submitter}`);
}
