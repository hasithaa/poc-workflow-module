// Slack Notifications for New Files in Google Drive
// Posts a Slack message when a new file/folder is created in Google Drive
// Inspired by: Workato Recipe - Slack notifications for new files/folders in Google Drive
// Reference: https://app.workato.com/recipes/685821-slack-notifications-for-new-files-folders-in-google-drive

import ballerinax/temporal;
import ballerinax/trigger.google.drive as drive;
import ballerinax/googleapis.drive as driveClient;
import ballerinax/slack;

// --- Type Definitions ---

type FileNotification record {|
    string fileId;
    string fileName;
    string mimeType;
    string fileUrl;
    string createdTime;
    string createdBy?;
    boolean isFolder;
|};

type NotificationResult record {|
    string fileId;
    boolean notified;
    string? slackMessageId;
|};

// --- Configuration ---

configurable string watchFolderId = ?;  // Google Drive folder to watch
configurable string slackChannel = "#file-notifications";
configurable string googleClientId = ?;
configurable string googleClientSecret = ?;
configurable string googleRefreshToken = ?;


// --- Workflow Definition ---

workflow driveSlackNotification on new temporal:Engine() {

    execute function notify(FileNotification file) returns NotificationResult|error {
        // Step 1: Format the notification message
        string message = check self->formatMessage(file);

        // Step 2: Post to Slack channel
        string messageId = check self->postToSlack(slackChannel, message, file);

        return {
            fileId: file.fileId,
            notified: true,
            slackMessageId: messageId
        };
    }

    activity function formatMessage(FileNotification file) returns string|error {
        // Format message based on file type
        // Example output:
        // "📄 New file uploaded: invoice_2024.pdf
        //  📁 Location: /Invoices/January
        //  👤 Created by: john@company.com
        //  🔗 Open file: https://drive.google.com/file/d/..."

        string icon = file.isFolder ? "📁" : "📄";
        string typeLabel = file.isFolder ? "folder" : "file";

        return string `${icon} New ${typeLabel} created: *${file.fileName}*
🔗 <${file.fileUrl}|Open in Google Drive>`;
    }

    activity function postToSlack(string channel, string message, FileNotification file) returns string|error {
        // Post message to Slack with file details
        // slack:Client sc = new;
        // slack:Message response = check sc->postMessage({
        //     channel: channel,
        //     text: message,
        //     attachments: [{
        //         color: "#4285F4",  // Google blue
        //         fields: [
        //             {title: "File Type", value: file.mimeType, short: true},
        //             {title: "Created", value: file.createdTime, short: true}
        //         ],
        //         actions: [{
        //             type: "button",
        //             text: "Open File",
        //             url: file.fileUrl
        //         }]
        //     }]
        // });
        // return response.ts;  // Slack message ID
    }
}

// --- Google Drive Event Handler Service ---

// --- Google Drive Trigger Listener ---

listener drive:Listener driveListener = new (listenerConfig = {
    clientId: googleClientId,
    clientSecret: googleClientSecret,
    refreshUrl: drive:REFRESH_URL,
    refreshToken: googleRefreshToken,
    callbackURL: "https://your-domain.com/drive/webhook",
    specificFolderOrFileId: watchFolderId
});


service drive:DriveService on driveListener {

    // Triggered when a new file is created
    remote function onFileCreate(drive:Change changeInfo) returns error? {
        string? fileId = changeInfo.fileId;
        if fileId is () {
            return error("Missing file ID");
        }

        FileNotification notification = {
            fileId: fileId,
            fileName: changeInfo.file?.name ?: "Unknown",
            mimeType: changeInfo.mimeType ?: "unknown",
            fileUrl: string `https://drive.google.com/file/d/${fileId}`,
            createdTime: changeInfo.time ?: "",
            isFolder: false
        };

        // Start the notification workflow
        _ = check driveSlackNotification->notify(notification);
    }

    // Triggered when a new folder is created
    remote function onFolderCreate(drive:Change changeInfo) returns error? {
        string? fileId = changeInfo.fileId;
        if fileId is () {
            return error("Missing folder ID");
        }

        FileNotification notification = {
            fileId: fileId,
            fileName: changeInfo.file?.name ?: "Unknown",
            mimeType: "application/vnd.google-apps.folder",
            fileUrl: string `https://drive.google.com/drive/folders/${fileId}`,
            createdTime: changeInfo.time ?: "",
            isFolder: true
        };

        // Start the notification workflow
        _ = check driveSlackNotification->notify(notification);
    }

    // Other events - not used in this workflow
    remote function onFileUpdate(drive:Change changeInfo) returns error? {
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