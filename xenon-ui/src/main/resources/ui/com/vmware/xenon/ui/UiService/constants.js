/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict'

var CONSTANTS = {
    'CONTROLLER_SUFFIX': "Controller",
    'VIEW': "View",
    'HTML': ".html",
    'DOC_DESCRIPTION': "documentDescription",
    'PROP_DESCRIPTIONS': "propertyDescriptions",
    'PROP_NAME': "propertyName",
    'QUERY_SRVC': "query-tasks",
    'OPERATION_INDEX_SVC': "operation-index",
    'MANAGEMENT_SVC': "management",
    'DOC_KIND': "documentKind",
    'MUST_OCCUR': "MUST_OCCUR",
    'SHOULD_OCCUR': "SHOULD_OCCUR",
    'MUST_NOT_OCCUR': "MUST_NOT_OCCUR",
    'WILDCARD': "WILDCARD",
    'PHRASE': "PHRASE",
    'TERM': "TERM",
    'EXPAND_CONTENT': "EXPAND_CONTENT",
    'DOCUMENT_SIGNATURE': "documentSignature",
    'TASK_STAGE': {
        'CREATED': "CREATED",
        'STARTED': "STARTED",
        'FINISHED': "FINISHED",
        'FAILED': "FAILED",
        'CANCELLED': "CANCELLED"
    },
    'ACTIONS': {
        'GET': "GET",
        'POST': "POST",
        'PATCH': "PATCH",
        'PUT': "PUT",
        'DELETE': "DELETE"
    },
    'CONTENT_TYPE': {
        'JSON': "application/json"
    },
    'KIND': {
        'SERIALIZED_OP': "com:vmware:dcp:common:Operation:SerializedOperation"
    },
    'SERVICE': {
        'PATH': '/core/ui'
    },
    'URI': {
        'QUERY_SRVC': "core/query-tasks",
        'OPERATION_INDEX_SVC': "core/operation-index",
        'MANAGEMENT_SVC': "core/management"
    },
    'UI_RESOURCES': '/user-interface/resources/com/vmware/xenon/ui/UiService/',
    'UI_BASE': '/core/ui/default#'
};
