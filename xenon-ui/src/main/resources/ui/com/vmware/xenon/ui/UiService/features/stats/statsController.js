/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('StatsController', ['$scope', 'StatsService', 'UtilService',
        '$routeParams',

        function ($scope, StatsService, UtilService, $routeParams) {
            var vm = this;
            vm.results = [];

            vm.chartOptions = {
                chart: {
                    type: 'discreteBarChart',
                    height: 300,
                    margin: {
                        top: 20,
                        right: 20,
                        bottom: 60,
                        left: 55
                    },
                    x: function (d) {
                        return d.label;
                    },
                    y: function (d) {
                        return d.value;
                    },
                    showValues: false,
                    valueFormat: function (d) {
                        return d3.format('d')(d);
                    },
                    transitionDuration: 500,
                    xAxis: {
                        axisLabel: 'Miliseconds'
                    },
                    yAxis: {
                        axisLabel: 'Request #',
                        axisLabelDistance: 30
                    },
                    color: ["#1595D3"]
                }
            };

            StatsService.getServiceStats($routeParams.path, $routeParams.serviceName,
                $routeParams.instanceId).then(function (response) {
                    var results = response.data.entries,
                        k = 0;
                    for (var prop in results) {
                        if (results[prop].logHistogram) {

                            var bins = [];
                            for (var i = 0; i < results[prop].logHistogram.bins.length; i++) {
                                bins[i] = {
                                    "label": "< 10^" + (i + 1),
                                    "value": results[prop].logHistogram.bins[i]
                                };
                            }
                            var formattedName = UtilService.formatCamelCaseString(results[prop].name);
                            vm.results[k] = [{
                                key: formattedName,
                                values: bins
                            }];
                            k++;
                        }
                    }
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });
        }]
);
