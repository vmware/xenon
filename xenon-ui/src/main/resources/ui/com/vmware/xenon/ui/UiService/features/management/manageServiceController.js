/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('ManageServiceController', ['$scope', 'ManageService',
        'HomeService', 'UtilService', '$routeParams',

        function ($scope, ManageService, HomeService, UtilService, $routeParams) {
            var vm = this;
            vm.documents = [];
            vm.documentTemplate = {};
            vm.newDocument = {};

            UtilService.getServiceTemplate($routeParams.selfLink).
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
                    vm.documentTemplate = templateDesc;
                    setNewDocument();
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });

            getServiceList();

            vm.createDocument = function () {
                ManageService.createService(vm.newDocument).
                    then(function (response) {
                        if (response.status === 200) {
                            $scope.notification.type = "success";
                            $scope.notification.text = "Service created successfully!";
                            setNewDocument();
                            getServiceList();
                        }
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
            };

            function setNewDocument() {
                vm.newDocument = angular.copy(vm.documentTemplate);
            };

            function getServiceList() {
                HomeService.getServiceDocuments($routeParams.selfLink).
                    then(function (response) {
                        vm.documents = response.data.documentLinks;
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
            };
        }]
);
