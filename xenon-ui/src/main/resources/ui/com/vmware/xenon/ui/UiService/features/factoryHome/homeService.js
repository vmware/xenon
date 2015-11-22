/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('HomeService', ['$http', 'UtilService',
    function ($http, UtilService) {
        this.getServiceDocuments = function (path, service) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/' + path + '/' + service,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.getServiceInstance = function (path, service, instance) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/' + path + '/' + service + '/' + instance,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };
    }]);