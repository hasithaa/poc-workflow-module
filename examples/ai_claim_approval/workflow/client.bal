import ballerina/file;
import ballerina/http;
import ballerina/io;

listener file:Listener fileListener = new (path = "./docs", recursive = false);

service file:Service on fileListener {
    remote function onCreate(file:FileEvent event) {

        do {
            // We have assumed the best case only for simplicity. In a production scenario, 
            // you would need to handle possible errors here.
            json claimJson = check io:fileReadJson(event.name);
            ClaimRequest claimRequest = check claimJson.cloneWithType();

            // Start the workflow for each claim request

            map<string> correlationData = {
                "reqId": claimRequest.id
            };

            // Tempory Signature WIP
            string workflowId = check workflowClient->startWorkflow({
                correlationData,
                workflowArgs: [
                    claimRequest.id,
                    claimRequest.user,
                    claimRequest.claims.toJsonString()
                ],
                taskQueue : "ClaimApprovalWorkflow"

            });
            io:println(string `[ClaimApprovalWorkflow] Started workflow with ID: ${workflowId} for user: ${claimRequest.user} and request ID: ${claimRequest.id}`);

        } on fail error e {
            io:println("Error processing file: ", e.message());
        }
    }

}

listener http:Listener httpDefaultListener = http:getDefaultListener();

service /document on httpDefaultListener {
    resource function get review(string reqId, string userId) returns http:Response|error {

        // Query the workflow for document details using the reqId
        map<string> correlationData = {reqId};
        // Let's use an untyped client for now
        anydata details = check workflowClient->query(correlationData, "getReviewDetails");

        ReviewDetails reviewDetails = check details.cloneWithType();

        http:Response res = new;
        string htmlForm = string `
        <html>
        <body>
            <h1>Claim Review ${reqId}</h1>
            <h2>Details for Review</h2>
            <p>User ID: ${userId}</p>
            <p>Manual Review Reason: ${reviewDetails.reason}</p>
            <h3>Claims:</h3>
            ${reviewDetails.claims}
            </ul>
            <br/>
            <p>Comment:</p>
            <textarea id="comment" rows="4" cols="50" placeholder="Enter your comment here..."></textarea>
            <br/><br/>
            <p>Actions:</p>
            <button onclick="sendApproval('${reqId}', '${userId}', true)">Approve</button>
            <button onclick="sendApproval('${reqId}', '${userId}', false)">Reject</button>
            
            <script>
            function sendApproval(reqId, userId, approved) {
            const comment = document.getElementById('comment').value;
            fetch('/document/approve', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                reqId: reqId,
                userId: userId,
                approved: approved,
                comment: comment
            })
            }).then(response => response.json())
              .then(data => alert('Response: ' + data.message))
              .catch(error => alert('Error: ' + error));
            }
            </script>
        </body>
        </html>
        `;
        res.setTextPayload(htmlForm);
        check res.setContentType("text/html");
        return res;
    }

    resource function post approve(ApprovalRequest approvalRequest) returns http:Response|error {

        // Notify the workflow about the approval decision
        map<string> correlationData = {reqId: approvalRequest.reqId};
        // Let's use an untyped client for now
        check workflowClient->signal(correlationData, "submitReview", {comment: approvalRequest.comment, result: approvalRequest.approved ? "APPROVED" : "REJECTED"});

        // Handle the approval submission
        http:Response res = new;
        json response = {
            "status": "success",
            "message": "Document " + (approvalRequest.approved ? "approved" : "rejected") + " successfully.",
            "reqId": approvalRequest.reqId
        };
        res.setJsonPayload(response);
        return res;
    }
}
