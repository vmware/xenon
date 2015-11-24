/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('HomeService', ['$http', 'UtilService',
    function ($http, UtilService) {
        this.getServiceDocuments = function (service) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/' + service,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.getServiceInstance = function (service) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/' + service,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };
    }]);
