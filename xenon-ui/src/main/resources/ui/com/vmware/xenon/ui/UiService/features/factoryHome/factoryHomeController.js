/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('FactoryHomeController', ['$scope', 'HomeService', 'UtilService',
        '$routeParams',

        function ($scope, HomeService, UtilService, $routeParams) {
            var vm = this;
            vm.documents = [];

            HomeService.getServiceDocuments($routeParams.selfLink).
                then(function (response) {
                    vm.documents = response.data.documentLinks;
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        }]
);
