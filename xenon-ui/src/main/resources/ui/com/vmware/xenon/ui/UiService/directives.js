/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

dcpDefault.directive('back', function factory($window) {
    return {
        restrict   : 'E',
        replace    : true,
        transclude : true,
        templateUrl: '<a href="#"><i class="glyphicon glyphicon-arrow-left"></i>' +
                    '<span>Back</span></a>',
        link: function (scope, element, attrs) {
            scope.navBack = function() {
                $window.history.back();
            };
        }
    };
});

dcpDefault.directive('timelineChart', function(){
    var chart = d3.custom.timelineChart();
    return {
        restrict: 'E',
        replace: true,
        template: '<div class="timelineChart"></div>',
        scope:{
            data: '=data'
        },
        link: function(scope, element, attrs) {
            var chartEl = d3.select(element[0]);
            scope.$watch('data', function (newVal, oldVal) {
                chartEl.datum(newVal).call(chart);
            });
        }
    }
});
