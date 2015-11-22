/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('SingletonHomeController', ['$scope', 'HomeService', 'UtilService',
        '$routeParams',

        function ($scope, HomeService, UtilService, $routeParams) {
            var vm = this;
            vm.serviceInstance = {};

            HomeService.getServiceInstance($routeParams.path, $routeParams.serviceName,
                $routeParams.instanceId).
                then(function (response) {
                    vm.serviceInstance = response.data;
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        }]
);
