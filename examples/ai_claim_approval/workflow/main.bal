import ballerina/io;
import hasitha/workflow;

// public function main() returns error? {
//     string s = check callStatisticalRiskAnalysisAgent( "[{\"claim\":\"C1\",\"amount\":\"3000\"},{\"claim\":\"C2\",\"amount\":\"2500\"},{\"claim\":\"C3\",\"amount\":\"1000\"}]");
//     io:println("Result: " + s);
// }

listener workflow:Listener workflowWorker = check new (workflowProvider, {taskQueue: "ClaimApprovalWorkflow"});

service "ClaimApprovalWorkflow" on workflowWorker {

    private string reqId = "";
    private string userId = "";
    private string claims = ""; // Should be Claim[]
    private string? reason = ();
    private string? userComment = ();
    private boolean isApproved = false;

    isolated remote function execute(workflow:Context ctx, string reqId, string userId, string claims) returns error|string|map<string> {
        self.reqId = reqId;
        self.userId = userId;
        self.claims = claims;

        anydata approvalResult = check ctx->callActivity("callStatisticalRiskAnalysisAgent", claims);
        self.reason = string:toLowerAscii(<string>approvalResult);

        if self.reason == "approve" || self.reason == "approved" {
            self.isApproved = true;
            return "APPROVED";
        } else {
            _ = check ctx->callActivity("sendManualApprovalRequest", "hasitha@wso2.com", reqId, userId);
            // Wait for user signal

            // Tempory Signature. Timeout details need to be optinoal
            _ = check ctx->awaitSignal("submitReview", 86400);
            if self.isApproved {
                return "APPROVED";
            } else {
                return {
                    "status": "REJECTED",
                    "reason": self.userComment ?: "Rejected by user without reason"
                };
            }
        }
    }

    @workflow:Query
    isolated resource function get ReviewDetails() returns ReviewDetails|error {
        io:println(string `[ClaimWF] ${self.reqId} : Retrieving review details`);
        if self.reason is () {
            return error("Review details not set yet");
        } else {
            return {
                reason: self.reason ?: "",
                claims: self.claims
            };
        }
    }

    // Need to fix the signature.
    @workflow:Signal
    isolated resource function post submitReview(map<anydata> signalData) returns error? {
        io:println(string `[ClaimWF] ${self.reqId} : on Submit Review`);

        SubmitReview submitReviewResult = check signalData.cloneWithType();

        self.userComment = submitReviewResult.comment;
        self.isApproved = submitReviewResult.result == "APPROVED";
    }
}
