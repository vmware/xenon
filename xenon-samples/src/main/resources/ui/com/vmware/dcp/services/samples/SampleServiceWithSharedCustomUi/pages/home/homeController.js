/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('customUiApp').controller('HomeController', ['$scope', 'HomeService', 'UtilService',
        '$routeParams', '$timeout',

        function ($scope, HomeService, UtilService, $routeParams, $timeout) {
            var vm = this;
            vm.documents = [];
            vm.factoryServices = [];
            vm.singletonServices = [];
            vm.nodeGroups = [];
            vm.system = {};
            vm.memory = [];
            vm.disk = [];
            vm.maintenanceInterval = 1000;
            vm.instance = {};

            vm.donutOptions = {
                chart: {
                    type: 'pieChart',
                    height: 250,
                    donut: true,
                    x: function (d) {
                        return d.key;
                    },
                    y: function (d) {
                        return d.y;
                    },
                    showLabels: false,
                    showLegend: false,
                    growOnHover: true,
                    color: ["#3DC3B3", "#2EA89B"],
                    pie: {
                        startAngle: function (d) {
                            return d.startAngle;
                        },
                        endAngle: function (d) {
                            return d.endAngle;
                        }
                    },
                    transitionDuration: 1000
                }
            };

            var poll = function() {
                $timeout(function() {
                    getSystemInfo();
                    poll();
                }, vm.maintenanceInterval);
            };

            poll();

            getServiceList();

            if($scope.instance !== undefined && $scope.instance !== ""){
                getServiceInstance();
            }


            function getSystemInfo() {
                HomeService.getSystemInfo($routeParams.path).then(function (response) {
                    vm.system = response.data;
                    vm.maintenanceInterval = vm.system.maintenanceIntervalMicros / 1000;
                    vm.disk = [
                        {
                            key: "Free Disk",
                            y: vm.system.systemInfo.freeDiskByteCount
                        },
                        {
                            key: "Used Disk",
                            y: vm.system.systemInfo.totalDiskByteCount - vm.system.systemInfo.freeDiskByteCount
                        }
                    ];

                    vm.memory = [
                        {
                            key: "Free Memory",
                            y: vm.system.systemInfo.freeMemoryByteCount
                        },
                        {
                            key: "Used Memory",
                            y: vm.system.systemInfo.totalMemoryByteCount - vm.system.systemInfo.freeMemoryByteCount
                        }
                    ];
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
            }

            $scope.deleteInstance = function (service) {
                HomeService.deleteService(service).
                    then(function (response) {
                        if (response.status === 200) {
                            $scope.notification.type = "success";
                            $scope.notification.text = "Instance deleted successfully.";
                            getServiceList();
                        }
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
            };

            function getServiceList() {
                HomeService.getServiceDocuments($scope.path, $scope.service).
                    then(function (response) {
                        vm.documents = response.data.documentLinks;
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
            };

            function getServiceInstance() {
                HomeService.getServiceInstance($scope.path, $scope.service, $scope.instance).
                    then(function (response) {
                        vm.instance = response.data;
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
            };

        }]
);
