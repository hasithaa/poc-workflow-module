import ballerina/http;
import ballerinax/temporal;

// --- Type Definitions ---

type OrderRequest record {|
    string id;
    string item;
|};

type PaymentMsg record {|
    string orderRef;
    decimal amount;
|};

// --- Workflow Declaration ---

workflow OrderFlow on new temporal:Engine() {

    // Workflow State
    string status = "NEW";

    // Execute Method - Entry point and main orchestration logic
    execute function process(OrderRequest req) returns string|error {
        self.status = "PROCESSING";

        // Activity Call - Invoke isolated unit of work
        int stock = check self->checkInventory(req.item);

        if stock > 0 {
            // Event Receive - Wait for external signal
            PaymentMsg payment = check self<-paymentReceived;
            self.status = "COMPLETED";
            return "Order completed successfully";
        }

        self.status = "FAILED";
        return "Order failed: Out of stock";
    } resolve {
        // correlation key extraction and setting as key.
        return req.id;
    }

    // Activity Method - Isolated unit of work
    activity function checkInventory(string itemId) returns int|error {
        // External service call or database query
        return 10;
    }

    // Event Method - Interface for receiving external signals
    event function paymentReceived(string orderRef, decimal amount) returns PaymentMsg|error {
        // Validation logic
        if amount <= 0 {
            return error("Invalid payment amount");
        }
        return { orderRef: orderRef, amount: amount };
    } resolve {
        // correlation key extraction
        return orderRef;
    }
}

// Usage Example

service /orderService on new http:Listener(8080) {

    resource function post placeOrder(OrderRequest req) returns string|error {
        // Start a new workflow instance
        string workflowId = check OrderFlow->process(req);
        return workflowId;
    }

    resource function post payment(string orderId, decimal amount) returns string|error {
        // Send payment event to the workflow
        check OrderFlow->paymentReceived(orderId, amount);
        return "Payment received";
    }
}