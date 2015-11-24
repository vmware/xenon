/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('ManageService', ['$http', 'UtilService', '$routeParams',
    function ($http, UtilService, $routeParams) {

        this.getServiceStats = function (service) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/' + service + '/stats',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.createService = function (newService) {
            var req = {
                method: 'POST',
                url: UtilService.getBaseUrl() + '/' + $routeParams.selfLink,
                data: newService,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.putService = function (service) {
            var req = {
                method: 'PUT',
                url: UtilService.getBaseUrl() + '/' + $routeParams.selfLink,
                data: service,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.patchService = function (service) {
            var req = {
                method: 'PATCH',
                url: UtilService.getBaseUrl() + $routeParams.selfLink,
                data: service,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.deleteService = function (callBody) {
            var req = {
                method: 'DELETE',
                url: UtilService.getBaseUrl() + $routeParams.serviceName
            };

            if (callBody) {
                req.data = callBody;
                req.headers = {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                };
            }
            return $http(req);
        };

    }]);
