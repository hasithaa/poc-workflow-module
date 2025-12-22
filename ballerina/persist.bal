// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/jballerina.java;

# Persistence provider manages Temporal client connection
# Shared by both Client and Listener
public isolated class PersistenceProvider {
    
    private handle temporalClient;
    
    # Initialize persistence provider with Temporal configuration
    #
    # + config - Temporal configuration
    # + return - Error if initialization fails
    public isolated function init(TemporalConfig config) returns error? {
        // io:println("[BPersistenceProvider] PersistenceProvider.init() called");
        self.temporalClient = check initTemporalClient(config);
        // io:println("[BPersistenceProvider] PersistenceProvider.init() completed");
    }
    
    # Get the underlying Temporal client handle
    #
    # + return - Temporal client handle
    isolated function getClientHandle() returns handle {
        lock {
            // io:println("[BPersistenceProvider] PersistenceProvider.getClientHandle() called");
            return self.temporalClient;
        }
    }
    
    # Close the Temporal client connection
    #
    # + return - Error if close fails
    public isolated function close() returns error? {
        lock {
            // io:println("[BPersistenceProvider] PersistenceProvider.close() called");
            error? result = closeTemporalClient(self.temporalClient);
            // io:println("[BPersistenceProvider] PersistenceProvider.close() completed");
            return result;
        }
    }
}

isolated function initTemporalClient(TemporalConfig config) returns handle|error = @java:Method {
    'class: "io.ballerina.stdlib.workflow.TemporalClientNative",
    name: "initClient"
} external;

isolated function closeTemporalClient(handle 'client) returns error? = @java:Method {
    'class: "io.ballerina.stdlib.workflow.TemporalClientNative",
    name: "closeClient"
} external;
