/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('ParentController', ['$scope', '$routeParams', 'UtilService',

        function ($scope, $routeParams, UtilService) {
            $scope.$on('$routeChangeSuccess', function () {
                $scope.path= $routeParams.path;
                $scope.service = $routeParams.serviceName;
                $scope.instance = $routeParams.instanceId;
                $scope.baseUrl = UtilService.getBaseUrl();
                $scope.uiBase = CONSTANTS.UI_BASE;
                $scope.notification = {};
            });

            $scope.onSetTime = function () {
                jQuery('.dropdown.open').removeClass('open');
            };
        }]
);
