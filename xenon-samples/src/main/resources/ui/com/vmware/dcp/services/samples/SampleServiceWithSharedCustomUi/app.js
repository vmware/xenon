/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';
/* App Module */
var customUiApp = angular.module('customUiApp', ['ngRoute', 'ngResource', 'nvd3', 'json-tree']);

customUiApp .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.
        when("/", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'pages/home/homeView.html'
        }).
        when("/:path/:serviceName/home", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'pages/home/homeView.html'
        }).
        when("/:path/:serviceName/:instanceId/home", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'pages/home/homeView.html'
        }).
        when("/:path/:serviceName/addService", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'pages/addPage/addService.html'
        }).
        otherwise({
            redirectTo: '/'
        });

}]);