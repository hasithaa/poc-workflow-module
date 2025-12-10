import ballerina/io;

// Document Approval Workflow Example
// This example demonstrates:
// - Waiting for any of multiple signals (approved/rejected/needsRevision)
// - Conditional logic based on received signals
// - Handling multi-path workflows

// Activity implementations for approval workflow
isolated function validateDocument(string documentId) returns error? {
    io:println(string `Validating document: ${documentId}`);
}

isolated function publishDocument(string documentId) returns error? {
    io:println(string `Publishing document: ${documentId}`);
}

isolated function notifySubmitter(string submitter, string message) returns error? {
    io:println(string `Notifying ${submitter}: ${message}`);
}
