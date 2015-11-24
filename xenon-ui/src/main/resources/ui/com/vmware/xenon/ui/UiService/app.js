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
        when("/:selfLink*/home", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/factoryHome/factoryView.html'
        }).
        when("/:selfLink*/instanceHome", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/singletonHome/singletonView.html'
        }).
        when("/:selfLink*/query", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/query/queryView.html"
        }).
        when("/:selfLink*/instanceQuery", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/query/queryView.html"
        }).
        when("/:selfLink*/stats", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/stats/statsView.html"
        }).
        when("/:selfLink*/instanceStats", {
            templateUrl: CONSTANTS.UI_RESOURCES + "features/stats/statsView.html"
        }).
        when("/:selfLink*/manage", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/manageServiceView.html'
        }).
        when("/:selfLink*/instancePut", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/putInstanceView.html'
        }).
        when("/:selfLink*/instancePatch", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/patchInstanceView.html'
        }).
        when("/:selfLink*/instanceDelete", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/management/deleteInstanceView.html'
        }).
        when("/core/operationIndex", {
            templateUrl: CONSTANTS.UI_RESOURCES + 'features/operationIndex/operationIndex.html'
        }).
        when("/404", {
            templateUrl: CONSTANTS.UI_RESOURCES + '404.html'
        }).
        otherwise({
            redirectTo: '/404'
        });

}]);
