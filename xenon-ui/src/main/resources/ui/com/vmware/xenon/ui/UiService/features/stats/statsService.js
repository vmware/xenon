/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('StatsService', ['$http', 'UtilService',
    function ($http, UtilService) {

        this.getServiceStats = function (path, service, instance) {
            var url;
            if (instance) {
                url = UtilService.getBaseUrl() + '/' + path + '/' + service + '/' + instance + '/stats';
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

    }]);