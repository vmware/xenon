/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('DashboardController', ['$scope', 'DashboardService', 'UtilService',
        '$routeParams',

        function ($scope, DashboardService, UtilService, $routeParams) {
            var vm = this;
            vm.factoryServices = [];
            vm.singletonServices = [];
            vm.nodeGroups = [];
            vm.system = {};
            vm.memory = [];
            vm.disk = [];
            vm.maintenanceInterval = 1000;

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

            setInterval(
                function(){
                    $scope.$apply(getSystemInfo());
                }, vm.maintenanceInterval);

            DashboardService.getServicesInstances().then(function (response) {
                vm.singletonServices = response.data.documentLinks;
            }, function (error) {
                $scope.notification.type = "danger";
                $scope.notification.text = error.data.message;
            });

            DashboardService.getFactoryServices().then(function (response) {
                vm.factoryServices = response.data.documentLinks;
            }, function (error) {
                $scope.notification.type = "danger";
                $scope.notification.text = error.data.message;
            });

            DashboardService.getNodeGroups().then(function (response) {
                vm.nodeGroups = response.data.documentLinks;
            }, function (error) {
                $scope.notification.type = "danger";
                $scope.notification.text = error.data.message;
            });

            function getSystemInfo() {
                DashboardService.getSystemInfo().then(function (response) {
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

        }]
);
