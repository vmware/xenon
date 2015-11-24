/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').service('UtilService', ['$http', '$location',
    function ($http, $location) {

        this.getServiceTemplate = function (service) {
            var req = {
                method: 'GET',
                url: this.getBaseUrl() + '/' + service + '/template',
                headers: {
                    'Content-Type': CONSTANTS.CONTENT_TYPE.JSON
                }
            };
            return $http(req);
        };


        this.getBaseUrl = function () {
            return $location.protocol() + '://' + $location.host() + ":" + $location.port();
        };

        this.formatCamelCaseString = function (camelCaseString) {
            var formattedText = "";
            if (camelCaseString.substr(0, 3) === "GET") {
                formattedText = "GET";
                camelCaseString = camelCaseString.substr(3);
            }
            if (camelCaseString.substr(0, 3) === "PUT") {
                formattedText = "PUT";
                camelCaseString = camelCaseString.substr(3);
            }
            if (camelCaseString.substr(0, 4) === "POST") {
                formattedText = "POST";
                camelCaseString = camelCaseString.substr(4);
            }
            if (camelCaseString.substr(0, 5) === "PATCH") {
                formattedText = "PATCH";
                camelCaseString = camelCaseString.substr(5);
            }
            if (camelCaseString.substr(0, 6) === "DELETE") {
                formattedText = "DELETE";
                camelCaseString = camelCaseString.substr(6);
            }

            camelCaseString = camelCaseString.replace(/([A-Z])/g, ' $1').
                replace(/^./, function (str) {
                    return str.toUpperCase();
                });

            return formattedText + " " + camelCaseString;
        };

        this.convertDateToEpoch = function (dateString) {
            return Date.parse(dateString)*1000;
        };

        this.getFactorySelfLink =  function (instance) {
            var lastIndex = instance.lastIndexOf("/");
            instance = instance.substring(0, lastIndex);
            return instance;
        };

    }]);
