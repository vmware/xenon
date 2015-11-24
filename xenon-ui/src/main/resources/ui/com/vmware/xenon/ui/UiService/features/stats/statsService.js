/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('StatsService', ['$http', 'UtilService',
    function ($http, UtilService) {

        this.getServiceStats = function (service, instance) {
            var url;
            if (instance) {
                url = UtilService.getBaseUrl() + '/' + service + '/' + instance + '/stats';
            } else {
                url = UtilService.getBaseUrl() + '/' + service + '/stats';
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
