// Invoice Processing Workflow
// Automates invoice receipt, validation, and payment processing
// Triggered by: Google Drive file upload (invoice PDF dropped in shared folder)
// Inspired by: Workato AP Automation recipes

import ballerinax/temporal;
import ballerinax/trigger.google.drive;
import ballerinax/quickbooks;
import ballerinax/slack;

// --- Type Definitions ---

type InvoiceDocument record {|
    string fileId;
    string fileName;
    string mimeType;
    string downloadUrl;
    string uploadedAt;
|};

type ExtractedInvoice record {|
    string invoiceNumber;
    string vendorName;
    string vendorEmail?;
    decimal amount;
    string currency;
    string dueDate;
    LineItem[] lineItems;
|};

type LineItem record {|
    string description;
    int quantity;
    decimal unitPrice;
    decimal total;
|};

type ValidationResult record {|
    boolean isValid;
    string[] errors;
    string[] warnings;
|};

type ProcessingResult record {|
    string invoiceId;
    string fileId;
    string status; // "approved", "pending_review", "rejected"
    string? rejectionReason;
|};

// --- Workflow Definition ---

workflow invoiceProcessing on new temporal:Engine() {

    string currentStatus = "received";

    execute function process(InvoiceDocument doc) returns ProcessingResult|error {
        // Step 1: Download file and extract data using OCR
        ExtractedInvoice extracted = check self->extractInvoiceData(doc.fileId, doc.mimeType);

        // Step 2: Validate extracted data
        ValidationResult validation = check self->validateInvoice(extracted);

        if !validation.isValid {
            // Notify AP team of validation errors
            check self->notifyValidationErrors(doc.fileName, extracted.invoiceNumber, validation.errors);
            // Move file to "Rejected" folder
            check self->moveToFolder(doc.fileId, "rejected");
            return {
                invoiceId: extracted.invoiceNumber,
                fileId: doc.fileId,
                status: "rejected",
                rejectionReason: validation.errors[0]
            };
        }

        // Step 3: Check for duplicate invoices
        boolean isDuplicate = check self->checkDuplicate(extracted.invoiceNumber, extracted.vendorName);
        if isDuplicate {
            check self->moveToFolder(doc.fileId, "duplicates");
            return {
                invoiceId: extracted.invoiceNumber,
                fileId: doc.fileId,
                status: "rejected",
                rejectionReason: "Duplicate invoice detected"
            };
        }

        // Step 4: Match with purchase orders
        string? poNumber = check self->matchPurchaseOrder(extracted);

        // Step 5: Create invoice in accounting system
        string invoiceId = check self->createInvoiceRecord(extracted, poNumber, doc.fileId);

        // Step 6: Route for approval based on amount
        string status = check self->routeForApproval(invoiceId, extracted.amount);

        // Step 7: Move file to "Processed" folder
        check self->moveToFolder(doc.fileId, "processed");

        return {invoiceId: invoiceId, fileId: doc.fileId, status: status, rejectionReason: ()};
    }

    activity function extractInvoiceData(string fileId, string mimeType) returns ExtractedInvoice|error {
        // Step 1: Download file from Google Drive
        // driveClient:Client dc = new;
        // byte[] fileContent = check dc->downloadFile(fileId);

        // Step 2: Use AWS Textract to extract invoice data
        // textract:Client tc = new;
        // textract:ExpenseAnalysis analysis = check tc->analyzeExpense(fileContent);

        // Step 3: Parse Textract response into ExtractedInvoice
        // return parseTextractResponse(analysis);
    }

    activity function validateInvoice(ExtractedInvoice invoice) returns ValidationResult|error {
        // Validate:
        // - Invoice number present and valid format
        // - Amount > 0
        // - Due date is valid and not in past
        // - Line items sum matches total (within tolerance)
        // - Vendor exists in approved vendor list
        // - Currency is supported
    }

    activity function notifyValidationErrors(string fileName, string invoiceNumber, string[] errors) returns error? {
        // Send Slack notification to AP team with file link
        // slack:Client sc = new;
        // sc->postMessage("#ap-team", {
        //     text: "Invoice validation failed",
        //     attachments: [{
        //         title: fileName,
        //         fields: errors.map(e => {title: "Error", value: e})
        //     }]
        // });
    }

    activity function checkDuplicate(string invoiceNumber, string vendorName) returns boolean|error {
        // Query accounting system for existing invoice
        // with same number from same vendor within last 12 months
        // quickbooks:Client qb = new;
        // return qb->findDuplicateBill(invoiceNumber, vendorName);
    }

    activity function matchPurchaseOrder(ExtractedInvoice invoice) returns string?|error {
        // Attempt to match invoice with open purchase orders
        // Match criteria: vendor name, amount (within 5% tolerance), line items
        // quickbooks:Client qb = new;
        // return qb->findMatchingPO(invoice.vendorName, invoice.amount);
    }

    activity function createInvoiceRecord(ExtractedInvoice invoice, string? poNumber, string fileId) returns string|error {
        // Create bill in QuickBooks with Drive file attachment
        // quickbooks:Client qb = new;
        // string billId = check qb->createBill({
        //     vendorName: invoice.vendorName,
        //     amount: invoice.amount,
        //     dueDate: invoice.dueDate,
        //     lineItems: invoice.lineItems,
        //     poNumber: poNumber,
        //     attachmentUrl: string `https://drive.google.com/file/d/${fileId}`
        // });
        // return billId;
    }

    activity function routeForApproval(string invoiceId, decimal amount) returns string|error {
        // Route based on amount thresholds:
        // - < $1000: Auto-approve
        // - $1000-$5000: Manager approval
        // - $5000-$25000: Director approval  
        // - > $25000: CFO approval
    }

    activity function moveToFolder(string fileId, string folderType) returns error? {
        // Move processed file to appropriate subfolder
        // folderType: "processed", "rejected", "duplicates", "pending_approval"
        // driveClient:Client dc = new;
        // string targetFolderId = getTargetFolder(folderType);
        // check dc->moveFile(fileId, targetFolderId);
    }
}

// --- Google Drive Event Handler Service ---
// Connects the Drive trigger to the workflow

// --- Google Drive Trigger Listener ---
// Watches a specific folder for new invoice uploads (PDF, images)

configurable string invoiceFolderId = ?;
configurable string googleClientId = ?;
configurable string googleClientSecret = ?;
configurable string googleRefreshToken = ?;

listener drive:Listener driveListener = check new (...);

service drive:DriveService on driveListener {

    // Triggered when a new file is uploaded to the invoice folder
    remote function onFileCreate(drive:Change changeInfo) returns error? {
        // Only process PDF and image files (invoices)
        string? mimeType = changeInfo.mimeType;
        string? fileId = changeInfo.fileId;

        if fileId is () {
            return error("Missing file ID in change event");
        }

        // Filter for invoice file types
        string[] invoiceMimeTypes = [
            "application/pdf",
            "image/png",
            "image/jpeg",
            "image/tiff"
        ];

        if mimeType is string && invoiceMimeTypes.indexOf(mimeType) != () {
            // Fetch file metadata
            InvoiceDocument doc = check fetchFileMetadata(fileId, mimeType);

            // Start the invoice processing workflow
            string workflowId = check invoiceProcessing->process(doc);
            // log:printInfo("Started invoice workflow", workflowId = workflowId, fileName = doc.fileName);
        }
    }

    // Triggered when a file is updated (re-uploaded)
    remote function onFileUpdate(drive:Change changeInfo) returns error? {
        // Optionally re-process updated invoices
        // Could be used for invoice corrections
    }

    // Required interface methods
    remote function onFolderCreate(drive:Change changeInfo) returns error? {
        return;
    }

    remote function onFolderUpdate(drive:Change changeInfo) returns error? {
        return;
    }

    remote function onDelete(drive:Change changeInfo) returns error? {
        return;
    }

    remote function onFileTrash(drive:Change changeInfo) returns error? {
        return;
    }

    remote function onFolderTrash(drive:Change changeInfo) returns error? {
        return;
    }
}

// --- Helper Functions ---

function fetchFileMetadata(string fileId, string mimeType) returns InvoiceDocument|error {
    // Fetch file details from Google Drive API
    // driveClient:Client dc = new;
    // driveClient:File file = check dc->getFile(fileId);
    // return {
    //     fileId: fileId,
    //     fileName: file.name,
    //     mimeType: mimeType,
    //     downloadUrl: file.webContentLink,
    //     uploadedAt: file.createdTime
    // };
    return error("Not implemented");
}