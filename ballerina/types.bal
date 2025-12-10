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

# Temporal configuration for workflow persistence
#
# + serviceUrl - Temporal server URL (default: localhost:7233)
# + namespace - Temporal namespace (default: default)
# + connectionTimeout - Connection timeout in seconds
# + identity - Optional worker identity
public type TemporalConfig record {|
    string serviceUrl = "localhost:7233";
    string namespace = "default";
    int connectionTimeout = 10;
    string identity?;
|};

# Listener configuration for workflow worker
#
# + taskQueue - Task queue name for this worker
# + maxConcurrentWorkflows - Maximum concurrent workflow executions
# + maxConcurrentActivities - Maximum concurrent activity executions
public type ListenerConfig record {|
    string taskQueue;
    int maxConcurrentWorkflows = 100;
    int maxConcurrentActivities = 100;
|};

# Workflow start parameters
#
# + correlationData - Key-value pairs for workflow instance correlation
# + workflowArgs - Arguments passed to workflow execute method
# + executionTimeout - Workflow execution timeout in seconds (0 = no timeout)
# + taskQueue - Task queue name where workflow should be executed (default: "default")
public type WorkflowStartParams record {|
    map<string> correlationData;
    anydata[] workflowArgs;
    int executionTimeout = 0;
    string taskQueue = "default";
|};

# Signal result containing signal name and data
#
# + signalName - Name of the received signal
# + data - Signal payload data
public type SignalResult record {|
    string signalName;
    map<string> data;
|};

# Workflow error
public type WorkflowError distinct error;

# Activity error
public type ActivityError distinct error;

# Timeout error
public type TimeoutError distinct error;

# Workflow handle for tracking execution
#
# + workflowId - Unique workflow instance ID
# + runId - Temporal run ID
public type WorkflowHandle record {|
    string workflowId;
    string runId;
|};

# Correlation mapping for workflow instance identification
#
# + keys - Array of correlation keys (in order)
# + separator - Separator character for correlation ID (default: "-")
public type CorrelationMapping record {|
    string[] keys;
    string separator = "-";
|};

# Signal data
#
# + signalName - Signal name
# + data - Signal payload
public type SignalData record {|
    string signalName;
    map<string> data;
|};

# Query result
#
# + value - Query result value
public type QueryResult record {|
    anydata value;
|};
