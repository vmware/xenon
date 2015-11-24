/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('ParentController', ['$scope', '$routeParams', 'UtilService', 'HomeService',

        function ($scope, $routeParams, UtilService, HomeService) {
            $scope.$on('$routeChangeSuccess', function () {
                $scope.service = $routeParams.selfLink;

                if ($scope.service) {
                    HomeService.getServiceInstance(UtilService.getFactorySelfLink($scope.service)).
                        then(function (response) {
                            $scope.instance = $routeParams.selfLink;
                            $scope.service = UtilService.getFactorySelfLink($scope.service);
                        }, function (error) {
                            $scope.instance = false;
                        });
                }

                $scope.baseUrl = UtilService.getBaseUrl();
                $scope.uiBase = CONSTANTS.UI_BASE;
                $scope.notification = {};
            });

            $scope.onSetTime = function () {
                jQuery('.dropdown.open').removeClass('open');
            };
        }]
);
