// HubSpot to Salesforce Contact Sync Workflow
// Triggered by HubSpot events, syncs contacts to Salesforce
// Inspired by: Workato HubSpot-Salesforce Integration
// Reference: https://www.workato.com/the-connector/getting-started-with-hubspot-and-salesforce-integration/

import ballerinax/temporal;
import ballerinax/trigger.hubspot;
import ballerinax/salesforce;

// --- Type Definitions ---

type HubSpotContact record {|
    string id;
    string email;
    string firstName;
    string lastName;
    string phone?;
    string company?;
    string lifecycleStage?;
|};

type SalesforceContact record {|
    string Id?;
    string Email;
    string FirstName;
    string LastName;
    string Phone?;
    string Account?;
    string LeadSource;
|};

type SyncResult record {|
    string hubspotContactId;
    string? salesforceContactId;
    string status; // "created", "updated", "skipped", "failed"
    string? errorMessage;
|};

// --- Workflow Definition ---

workflow hubSpotToSalesforceSync on new temporal:Engine() {

    string syncStatus = "pending";

    execute function syncContact(HubSpotContact contact) returns SyncResult|error {
        // Step 1: Validate the incoming contact data
        check self->validateContact(contact);

        // Step 2: Check if contact already exists in Salesforce
        SalesforceContact? existingContact = check self->findSalesforceContact(contact.email);

        if existingContact is SalesforceContact {
            // Step 3a: Update existing Salesforce contact
            check self->updateSalesforceContact(existingContact.Id ?: "", contact);
            self.syncStatus = "updated";
            return {
                hubspotContactId: contact.id,
                salesforceContactId: existingContact.Id,
                status: "updated",
                errorMessage: ()
            };
        } else {
            // Step 3b: Create new contact in Salesforce
            string newContactId = check self->createSalesforceContact(contact);
            self.syncStatus = "created";
            return {
                hubspotContactId: contact.id,
                salesforceContactId: newContactId,
                status: "created",
                errorMessage: ()
            };
        }
    }

    activity function validateContact(HubSpotContact contact) returns error? {
        // Validate required fields:
        // - Email must be present and valid format
        // - LastName is required for Salesforce
        // - Check for duplicate prevention rules
    }

    activity function findSalesforceContact(string email) returns SalesforceContact?|error {
        // Query Salesforce for existing contact by email
        // salesforce:Client sf = new;
        // string query = string `SELECT Id, Email, FirstName, LastName, Phone 
        //                        FROM Contact WHERE Email = '${email}' LIMIT 1`;
        // return sf->query(query);
    }

    activity function createSalesforceContact(HubSpotContact hubspotContact) returns string|error {
        // Map HubSpot fields to Salesforce fields and create contact
        // SalesforceContact sfContact = {
        //     Email: hubspotContact.email,
        //     FirstName: hubspotContact.firstName,
        //     LastName: hubspotContact.lastName,
        //     Phone: hubspotContact.phone,
        //     LeadSource: "HubSpot"
        // };
        // salesforce:Client sf = new;
        // return sf->create("Contact", sfContact);
    }

    activity function updateSalesforceContact(string contactId, HubSpotContact hubspotContact) returns error? {
        // Update existing Salesforce contact with HubSpot data
        // salesforce:Client sf = new;
        // return sf->update("Contact", contactId, {
        //     FirstName: hubspotContact.firstName,
        //     LastName: hubspotContact.lastName,
        //     Phone: hubspotContact.phone
        // });
    }
}

// --- HubSpot Event Handler Service ---
// Connects the HubSpot trigger to the workflow

// --- HubSpot Trigger Listener ---
// Listens for HubSpot webhook events (contact creation, update, deletion)

listener hubspot:Listener hubspotWebhook = new (listenerConfig = {
    clientSecret: "<HUBSPOT_CLIENT_SECRET>",
    callbackURL: "https://your-domain.com/hubspot/webhook"
});


service hubspot:ContactService on hubspotWebhook {

    // Triggered when a new contact is created in HubSpot
    remote function onContactCreation(hubspot:WebhookEvent event) returns error? {
        // Extract contact ID from the webhook event
        decimal? objectId = event.objectId;
        if objectId is () {
            return error("Missing contact ID in webhook event");
        }

        // Fetch full contact details from HubSpot API
        // (webhook only contains the ID, not the full contact data)
        HubSpotContact contact = check fetchHubSpotContactDetails(objectId.toString());

        // Start the sync workflow
        string workflowId = check hubSpotToSalesforceSync->syncContact(contact);
        // log:printInfo("Started sync workflow", workflowId = workflowId, contactId = contact.id);
    }

    // Triggered when a contact property changes in HubSpot
    remote function onContactPropertychange(hubspot:WebhookEvent event) returns error? {
        decimal? objectId = event.objectId;
        if objectId is () {
            return error("Missing contact ID in webhook event");
        }

        // Only sync on specific property changes (email, name, phone)
        string? propertyName = event.propertyName;
        string[] syncProperties = ["email", "firstname", "lastname", "phone"];

        if propertyName is string && syncProperties.indexOf(propertyName) != () {
            HubSpotContact contact = check fetchHubSpotContactDetails(objectId.toString());
            _ = check hubSpotToSalesforceSync->syncContact(contact);
        }
    }

    // Triggered when a contact is deleted in HubSpot
    remote function onContactDeletion(hubspot:WebhookEvent event) returns error? {
        // Optionally handle deletion - could mark as inactive in Salesforce
        // or remove the contact based on business requirements
    }
}

// --- Helper Functions ---

function fetchHubSpotContactDetails(string contactId) returns HubSpotContact|error {
    // Fetch full contact details from HubSpot API
    // hubspotClient:Client hs = new;
    // hubspotClient:Contact contact = check hs->getContact(contactId);
    // return {
    //     id: contact.id,
    //     email: contact.email,
    //     firstName: contact.firstname,
    //     lastName: contact.lastname,
    //     phone: contact.phone,
    //     company: contact.company,
    //     lifecycleStage: contact.lifecyclestage
    // };
    return error("Not implemented");
}