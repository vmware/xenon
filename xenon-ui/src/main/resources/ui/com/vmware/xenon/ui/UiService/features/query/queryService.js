/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('QueryService', ['$http', 'UtilService',
    function ($http, UtilService) {

        this.postQuery = function (querySpec) {
            var req = {
                method: 'POST',
                url: UtilService.getBaseUrl() + '/' + CONSTANTS.URI.QUERY_SRVC,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                },
                data: querySpec
            };
            return $http(req);
        };

        this.getQueryResults = function (queryPath) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + queryPath
            };
            return $http(req);
        }

    }]);
