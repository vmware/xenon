/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';
/* App Module */
var dcpDefault = angular.module('dcpDefault', ['ngRoute', 'ngResource', 'nvd3', 'json-tree',
    'ngSanitize', 'MassAutoComplete', 'ui.bootstrap.datetimepicker']);

dcpDefault.config(['$routeProvider', function ($routeProvider) {
    $routeProvider.
        when("/", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/dashboard/dashboardView.html'
        }).
        when("/:path/:serviceName/home", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/factoryHome/factoryView.html'
        }).
        when("/:path/:serviceName/:instanceId/home", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/singletonHome/singletonView.html'
        }).
        when("/:path/:serviceName/query", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/query/queryView.html"
        }).
        when("/:path/:serviceName/:instanceId/query", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/query/queryView.html"
        }).
        when("/:path/:serviceName/stats", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/stats/statsView.html"
        }).
        when("/:path/:serviceName/:instanceId/stats", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/stats/statsView.html"
        }).
        when("/:path/:serviceName/manage", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/manageServiceView.html'
        }).
        when("/:path/:serviceName/:instanceId/put", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/putInstanceView.html'
        }).
        when("/:path/:serviceName/:instanceId/patch", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/patchInstanceView.html'
        }).
        when("/:path/:serviceName/:instanceId/delete", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/deleteInstanceView.html'
        }).
        when("/:path/operationIndex", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/operationIndex/operationIndex.html'
        }).
        when("/404", {
            templateUrl: CONSTANTS.UI_RESOURCES + '404.html'
        }).
        otherwise({
            redirectTo: '/404'
        });

}]);
