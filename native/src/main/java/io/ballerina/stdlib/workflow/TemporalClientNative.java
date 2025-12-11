/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.workflow;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.creators.ErrorCreator;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;

/**
 * Native implementation for Temporal client initialization and lifecycle.
 */
public class TemporalClientNative {

    /**
     * Initialize Temporal client with configuration.
     *
     * @param config Ballerina TemporalConfig record
     * @return WorkflowClient handle
     */
    public static Object initClient(BMap<BString, Object> config) {
        System.out.println("[JTemporal] ========== initClient() ENTRY ==========");
        try {
            String serviceUrl = config.get(StringUtils
                    .fromString("serviceUrl")).toString();
            String namespace = config.get(StringUtils
                    .fromString("namespace")).toString();
            long connectionTimeout = (long) config.get(StringUtils
                    .fromString("connectionTimeout"));

            System.out.println("[JTemporal] Configuration - serviceUrl: " + serviceUrl);
            System.out.println("[JTemporal] Configuration - namespace: " + namespace);
            System.out.println("[JTemporal] Configuration - connectionTimeout: " + connectionTimeout);

            // Build service stubs options
            System.out.println("[JTemporal] Building WorkflowServiceStubsOptions...");
            WorkflowServiceStubsOptions.Builder stubsOptionsBuilder = WorkflowServiceStubsOptions.newBuilder();
            
            if (serviceUrl != null && !serviceUrl.isEmpty()) {
                System.out.println("[JTemporal] Setting target: " + serviceUrl);
                stubsOptionsBuilder.setTarget(serviceUrl);
            }

            System.out.println("[JTemporal] Creating WorkflowServiceStubs...");
            WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubsOptionsBuilder.build());
            System.out.println("[JTemporal] WorkflowServiceStubs created successfully");

            // Build workflow client options
            System.out.println("[JTemporal] Building WorkflowClientOptions...");
            WorkflowClientOptions.Builder clientOptionsBuilder = WorkflowClientOptions.newBuilder()
                    .setNamespace(namespace);

            // Check for optional identity
            if (config.containsKey(StringUtils.fromString("identity"))) {
                String identity = config.get(StringUtils
                        .fromString("identity")).toString();
                if (identity != null && !identity.isEmpty()) {
                    System.out.println("[JTemporal] Setting identity: " + identity);
                    clientOptionsBuilder.setIdentity(identity);
                }
            }

            System.out.println("[JTemporal] Creating WorkflowClient instance...");
            WorkflowClient client = WorkflowClient.newInstance(service, clientOptionsBuilder.build());
            System.out.println("[JTemporal] WorkflowClient created successfully");
            System.out.println("[JTemporal] ========== initClient() EXIT [SUCCESS] ==========");
            
            return client;
        } catch (Exception e) {
            System.err.println("[JTemporal] ========== initClient() EXIT [ERROR] ==========");
            System.err.println("[JTemporal] Error initializing Temporal client: " + e.getMessage());
            return ErrorCreator.createError(
                    StringUtils.fromString(
                            "Failed to initialize Temporal client: " + e.getMessage()));
        }
    }

    /**
     * Close Temporal client connection.
     *
     * @param clientHandle WorkflowClient handle
     * @return null on success, error on failure
     */
    public static Object closeClient(Object clientHandle) {
        System.out.println("[JTemporal] ========== closeClient() ENTRY ==========");
        try {
            if (clientHandle instanceof WorkflowClient) {
                System.out.println("[JTemporal] Client handle is valid WorkflowClient");
                // WorkflowClient in newer versions doesn't have close() method
                // Cleanup is handled automatically
                // WorkflowClient client = (WorkflowClient) clientHandle;
            }
            System.out.println("[JTemporal] ========== closeClient() EXIT [SUCCESS] ==========");
            return null;
        } catch (Exception e) {
            System.err.println("[JTemporal] ========== closeClient() EXIT [ERROR] ==========");
            System.err.println("[JTemporal] Error closing Temporal client: " + e.getMessage());
            return ErrorCreator.createError(
                    StringUtils.fromString(
                            "Failed to close Temporal client: " + e.getMessage()));
        }
    }
}
