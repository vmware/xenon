/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.services.common;

import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.WebSocketService;

public class ServiceUriPaths {
    public static final String SERVICE_URI_SUFFIX_SYNCHRONIZATION = "synch";

    public static final String SERVICE_URI_SUFFIX_FORWARDING = "forwarding";

    public static final String SERVICE_URI_SUFFIX_REPLICATION = "replication";

    public static final String CORE = "/core";
    public static final String UI_PATH_SUFFIX = "/ui";

    public static final String CORE_WEB_SOCKET_ENDPOINT = CORE + "/ws-endpoint";
    public static final String WEB_SOCKET_SERVICE_PREFIX = "/ws-service";

    public static final String MANAGEMENT = "/management";
    public static final String CORE_MANAGEMENT = CORE + MANAGEMENT;
    public static final String CORE_CALLBACKS = CORE + "/callbacks";
    public static final String PROCESS_LOG = CORE_MANAGEMENT + "/process-log";
    public static final String GO_PROCESS_LOG = CORE_MANAGEMENT + "/go-dcp-process-log";
    public static final String SYSTEM_LOG = CORE_MANAGEMENT + "/system-log";
    public static final String MIGRATION_TASKS = MANAGEMENT + "/migration-tasks";

    public static final String CORE_PROCESSES = CORE + "/processes";

    public static final String COORDINATED_UPDATE_FACTORY = CORE + "/coordinated-updates";
    public static final String NODE_GROUP_FACTORY = CORE + "/node-groups";
    public static final String DEFAULT_NODE_GROUP_NAME = "default";
    public static final String DEFAULT_NODE_GROUP = NODE_GROUP_FACTORY + "/"
            + DEFAULT_NODE_GROUP_NAME;

    public static final String NODE_SELECTOR_PREFIX = CORE + "/node-selectors";
    public static final String DEFAULT_NODE_SELECTOR_NAME = "default";
    public static final String DEFAULT_NODE_SELECTOR = NODE_SELECTOR_PREFIX + "/"
            + DEFAULT_NODE_SELECTOR_NAME;
    public static final String DEFAULT_3X_NODE_SELECTOR = NODE_SELECTOR_PREFIX + "/"
            + DEFAULT_NODE_SELECTOR_NAME + "-3x";

    public static final String CORE_AUTH = CORE + "/auth";
    public static final String CORE_CREDENTIALS = CORE_AUTH + "/credentials";

    public static final String CORE_DOCUMENT_INDEX = ServiceUriPaths.CORE + "/document-index";
    public static final String CORE_OPERATION_INDEX = ServiceUriPaths.CORE + "/operation-index";
    public static final String CORE_SERVICE_CONTEXT_INDEX = ServiceUriPaths.CORE
            + "/service-context-index";
    public static final String CORE_BLOB_INDEX = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "blob-index");

    public static final String CORE_QUERY_TASKS = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "query-tasks");

    public static final String CORE_GRAPH_QUERIES = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "graph-queries");

    public static final String ODATA_QUERIES = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "odata-queries");

    public static final String CORE_LOCAL_QUERY_TASKS = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "local-query-tasks");

    public static final String CORE_AUTHZ = UriUtils.buildUriPath(ServiceUriPaths.CORE, "authz");
    public static final String CORE_AUTHZ_USER_GROUPS = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "user-groups");
    public static final String CORE_AUTHZ_RESOURCE_GROUPS = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "resource-groups");
    public static final String CORE_AUTHZ_ROLES = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "roles");
    public static final String CORE_AUTHZ_USERS = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "users");
    public static final String CORE_AUTHZ_VERIFICATION = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "verification");
    public static final String CORE_AUTHZ_SYSTEM_USER = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "system-user");
    public static final String CORE_AUTHZ_GUEST_USER = UriUtils.buildUriPath(
            ServiceUriPaths.CORE_AUTHZ, "guest-user");

    public static final String CORE_AUTHN = UriUtils.buildUriPath(ServiceUriPaths.CORE, "authn");
    public static final String CORE_AUTHN_BASIC = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHN,
            "basic");

    public static final String CORE_TRANSACTIONS = UriUtils.buildUriPath(ServiceUriPaths.CORE,
            "transactions");

    public static final String UI_RESOURCES = "/user-interface/resources";

    public static final String UI_RESOURCE_DEFAULT_FILE = "index.html";
    public static final String UI_SERVICE_CORE_PATH = ServiceUriPaths.CORE + "/ui/default";
    public static final String UI_SERVICE_BASE_URL = UI_SERVICE_CORE_PATH + "/#";
    public static final String UI_SERVICE_HOME = "/home";
    public static final String CUSTOM_UI_BASE_URL = ServiceUriPaths.CORE + "/ui/custom";
    public static final String SAMPLES = "/samples";
    public static final String DNS = CORE + "/dns";

    public static final String WS_SERVICE_LIB_JS = "/ws-service-lib.js";
    public static final String WS_SERVICE_LIB_JS_PATH =
            Utils.buildUiResourceUriPrefixPath(WebSocketService.class) + "/ws-service-lib.js";

    /**
     * Swagger discovery service is started on this URI.
     * @see com.vmware.xenon.swagger.SwaggerDescriptorService
     */
    public static final String SWAGGER = "/discovery/swagger";
}
