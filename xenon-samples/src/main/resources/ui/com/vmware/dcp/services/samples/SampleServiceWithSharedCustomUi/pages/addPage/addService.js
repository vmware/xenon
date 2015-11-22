/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('customUiApp').service('AddService', ['$http', 'UtilService', '$routeParams',
    function ($http, UtilService, $routeParams) {

        this.getServiceStats = function (path, service, instance) {
            var url;
            if (instance) {
                url = UtilService.getBaseUrl() + '/' + path + '/' + service + '/' + instance
                + '/stats';
            } else {
                url = UtilService.getBaseUrl() + '/' + path + '/' + service + '/stats';
            }

            var req = {
                method: 'GET',
                url: url,
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.createService = function (newService) {
            var req = {
                method: 'POST',
                url: UtilService.getBaseUrl() + '/' + $routeParams.path + '/'
                + $routeParams.serviceName,
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
                url: UtilService.getBaseUrl() + '/' + $routeParams.path + '/'
                + $routeParams.serviceName + '/' + $routeParams.instanceId,
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
                url: UtilService.getBaseUrl() + '/' + $routeParams.path + '/'
                + $routeParams.serviceName + '/' + $routeParams.instanceId,
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
                url: UtilService.getBaseUrl() + '/' + $routeParams.path + '/'
                + $routeParams.serviceName + '/' + $routeParams.instanceId
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