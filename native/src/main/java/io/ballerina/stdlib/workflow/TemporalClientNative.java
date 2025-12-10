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
        try {
            String serviceUrl = config.get(StringUtils
                    .fromString("serviceUrl")).toString();
            String namespace = config.get(StringUtils
                    .fromString("namespace")).toString();
            long connectionTimeout = (long) config.get(StringUtils
                    .fromString("connectionTimeout"));

            // Build service stubs options
            WorkflowServiceStubsOptions.Builder stubsOptionsBuilder = WorkflowServiceStubsOptions.newBuilder();
            
            if (serviceUrl != null && !serviceUrl.isEmpty()) {
                stubsOptionsBuilder.setTarget(serviceUrl);
            }

            WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubsOptionsBuilder.build());

            // Build workflow client options
            WorkflowClientOptions.Builder clientOptionsBuilder = WorkflowClientOptions.newBuilder()
                    .setNamespace(namespace);

            // Check for optional identity
            if (config.containsKey(StringUtils.fromString("identity"))) {
                String identity = config.get(StringUtils
                        .fromString("identity")).toString();
                if (identity != null && !identity.isEmpty()) {
                    clientOptionsBuilder.setIdentity(identity);
                }
            }

            WorkflowClient client = WorkflowClient.newInstance(service, clientOptionsBuilder.build());
            
            return client;
        } catch (Exception e) {
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
        try {
            if (clientHandle instanceof WorkflowClient) {
                // WorkflowClient in newer versions doesn't have close() method
                // Cleanup is handled automatically
                // WorkflowClient client = (WorkflowClient) clientHandle;
            }
            return null;
        } catch (Exception e) {
            return ErrorCreator.createError(
                    StringUtils.fromString(
                            "Failed to close Temporal client: " + e.getMessage()));
        }
    }
}
