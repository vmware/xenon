/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('OperationIndexService', ['$http', 'UtilService', '$routeParams',
    function ($http, UtilService, $routeParams) {

        this.checkServiceIsStarted = function(){
            var req = {
                method: CONSTANTS.ACTIONS.GET,
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.OPERATION_INDEX_SVC + '?documentSelfLink=*',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.startService = function () {
            var req = {
                method: CONSTANTS.ACTIONS.PATCH,
                data: {
                    'enable': "START",
                    'kind': "com:vmware:dcp:services:common:ServiceHostManagementService:ConfigureOperationTracingRequest"
                },
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.MANAGEMENT_SVC,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.stopService = function () {
            var req = {
                method: CONSTANTS.ACTIONS.PATCH,
                data: {
                    'enable': "STOP",
                    'kind': "com:vmware:dcp:services:common:ServiceHostManagementService:ConfigureOperationTracingRequest"
                },
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.MANAGEMENT_SVC,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.postQuery = function (querySpec) {
            var req = {
                method: CONSTANTS.ACTIONS.POST,
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.QUERY_SRVC,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                },
                data: querySpec
            };
            return $http(req);
        };

        this.getOperationsIndex = function (querySpec) {
            var req = {
                method: CONSTANTS.ACTIONS.POST,
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.QUERY_SRVC,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                },
                data: querySpec
            };
            return $http(req);
        };

        this.getPaginatedResults = function (nextPageLink) {
            var req = {
                method: CONSTANTS.ACTIONS.GET,
                url: UtilService.getBaseUrl() + nextPageLink,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };
    }]);
