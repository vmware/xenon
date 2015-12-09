/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('customUiApp').service('HomeService', ['$http', 'UtilService',
    function ($http, UtilService) {

        this.getSystemInfo = function (path) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/core/management',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.getServicesInstances = function (path) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/core/document-index?documentSelfLink=*',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.getNodeGroups = function (path) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/core/node-groups',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };

        this.getFactoryServices = function (path) {
            var req = {
                method: 'GET',
                url: UtilService.getBaseUrl() + '/',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        }


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


        this.deleteService = function (selflink) {
            var req = {
                method: 'DELETE',
                data: {},
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                },
                url: UtilService.getBaseUrl() + selflink
            };
            return $http(req);
        };


    }]);