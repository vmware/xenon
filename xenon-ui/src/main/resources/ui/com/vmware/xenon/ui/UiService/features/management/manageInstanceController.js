/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('ManageInstanceController', ['$scope', 'ManageService',
    'HomeService', 'UtilService', '$routeParams', '$location',

    function ($scope, ManageService, HomeService, UtilService, $routeParams, $location) {
        var vm = this;
        vm.document = {};
        vm.permDelete = false;
        vm.patchDoc = {};
        vm.documentKeys = [];

        UtilService.getServiceTemplate($routeParams.path, $routeParams.serviceName).
            then(function (response) {
                var templateDesc = response.data.documents;

                for (var prop in templateDesc) {
                    templateDesc = templateDesc[prop];
                }
                delete templateDesc.documentDescription;
                delete templateDesc.documentExpirationTimeMicros;
                delete templateDesc.documentKind;
                delete templateDesc.documentSelfLink;
                delete templateDesc.documentUpdateTimeMicros;
                delete templateDesc.documentVersion;
                vm.document = templateDesc;
                vm.documentKeys = Object.keys(vm.document);
            }, function (error) {
                $scope.notification.type = "danger";
                $scope.notification.text = error.data.message;
            });

        vm.putDocument = function () {
            ManageService.putService(vm.document).
                then(function (response) {
                    if (response.status === 200) {
                        $scope.notification.type = "success";
                        $scope.notification.text = "Instance updated successfully.";
                    }
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        };

        vm.patchDocument = function () {
            ManageService.patchService(vm.patchDoc).
                then(function (response) {
                    if (response.status === 200) {
                        $scope.notification.type = "success";
                        $scope.notification.text = "Instance updated successfully.";
                    }
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        };

        vm.deleteDocument = function () {
            var deleteBody;
            if (vm.permDelete) {
                deleteBody = {};
            }

            ManageService.deleteService(deleteBody).
                then(function (response) {
                    if (response.status === 200) {
                        $scope.notification.type = "success";
                        $scope.notification.text = "Instance deleted successfully.";
                        $location.path('/' + $routeParams.path + '/' + $routeParams.serviceName
                        + "/home");
                    }
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        };
    }]);
