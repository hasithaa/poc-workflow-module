import ballerina/http;

import hasitha/workflow;

// Register activities function, This is to mock activity registration, which is done implicitly by the workflow module.
// Initialize activities at module load time
function init() returns error? {
    check workflow:registerActivity("validateDocument", validateDocument);
}

isolated function validateDocument(string documentId) returns error|string {

    // Mock validation logic
    http:Client cl = check new ("http://localhost:9090/workflows");
    string j = check cl->get("/health");

    return j;
}
