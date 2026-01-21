import ballerina/http;
import ballerinax/temporal;


// --- Type Definitions ---
type OrderRequest record {|
    string id;
    string item;
|};

// --- Workflow Declaration ---
workflow orderFlow on new temporal:Engine() {

    // Workflow State
    string status = "NEW";

    // Execute Method - Entry point and main orchestration logic
    execute function process(OrderRequest req) returns string|error {
        self.status = "PROCESSING";

        // Activity Call - Invoke isolated unit of work
        int stock = check self->checkInventory(req.item);

        if stock > 0 {
            self.status = "COMPLETED";
            return "Order completed successfully";
        }
        self.status = "FAILED";
        return "Order failed: Out of stock";
    }

    // Activity Method - Isolated unit of work
    activity function checkInventory(string itemId) returns int|error {
        // External service call or database query
        return 10;
    }
}

// Usage Example

service /orderService on new http:Listener(8080) {

    resource function post placeOrder(OrderRequest req) returns string|error {
        // Start a new workflow instance
        string workflowId = check orderFlow->process(req);
        return workflowId;
    }
}   