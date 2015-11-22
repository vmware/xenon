/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('OperationIndexController', ['$scope', 'OperationIndexService',
    'UtilService',

    function ($scope, OperationIndexService, UtilService) {
        var vm = this;
        vm.queryOptions = {
            'referer': "",
            'host': "",
            'port': "",
            'path': "",
            'action ': "",
            'jsonBody': "",
            'contextId': "",
            'fromTime': "",
            'toTime': ""
        };

        vm.referrerUris = [];
        vm.hostsList = [];
        vm.portsList = [];
        vm.destinationPaths = [];
        vm.contextIds = [];
        vm.operationsList = [];
        vm.serviceStarted = false;

        vm.actions = [
            {id: CONSTANTS.ACTIONS.GET, name: CONSTANTS.ACTIONS.GET},
            {id: CONSTANTS.ACTIONS.POST, name: CONSTANTS.ACTIONS.POST},
            {id: CONSTANTS.ACTIONS.PATCH, name: CONSTANTS.ACTIONS.PATCH},
            {id: CONSTANTS.ACTIONS.PUT, name: CONSTANTS.ACTIONS.PUT},
            {id: CONSTANTS.ACTIONS.DELETE, name: CONSTANTS.ACTIONS.DELETE}
        ];

        //Start operation indexing service if started it will fail
        OperationIndexService.startService().then(function (response) {
            vm.serviceStarted = true;
            populateAutocomplete();
        }, function (error) {
            if(error.data.status === 500) {
                $scope.notification.type = "danger";
                vm.serviceStarted = false;
            } else {
                populateAutocomplete();
                vm.serviceStarted = true;
                $scope.notification.type = "info";
            }

            $scope.notification.text = error.data.message;
        });

        // Required support for autocomplete
        function suggest_referrers(term) {
            var q = term.toLowerCase().trim();
            var results = [];

            // Find first 10 states that start with `term`.
            for (var i = 0; i < vm.referrerUris.length && results.length < 10; i++) {
                var referrerUri = vm.referrerUris[i];
                if (referrerUri.toLowerCase().indexOf(q) === 0) {
                    results.push({label: referrerUri, value: referrerUri});
                }
            }
            return results;
        }

        vm.referrer_options = { suggest: suggest_referrers };

        function suggest_hosts(term) {
            var q = term.toLowerCase().trim();
            var results = [];

            // Find first 10 states that start with `term`.
            for (var i = 0; i < vm.hostsList.length && results.length < 10; i++) {
                var host = vm.hostsList[i];
                if (host.indexOf(q) === 0)
                    results.push({label: host, value: host});
            }
            return results;
        }

        vm.host_options = { suggest: suggest_hosts };

        function suggest_ports(term) {
            var q = term.toLowerCase().trim();
            var results = [];

            // Find first 10 states that start with `term`.
            for (var i = 0; i < vm.portsList.length && results.length < 10; i++) {
                var port = vm.portsList[i];
                if (port.toString().indexOf(q) === 0)
                    results.push({label: port, value: port});
            }
            return results;
        }

        vm.port_options = { suggest: suggest_ports };


        function suggest_paths(term) {
            var q = term.toLowerCase().trim();
            var results = [];

            // Find first 10 states that start with `term`.
            for (var i = 0; i < vm.destinationPaths.length && results.length < 10; i++) {
                var destinationUri = vm.destinationPaths[i];
                if (destinationUri.toLowerCase().indexOf(q) === 0)
                    results.push({label: destinationUri, value: destinationUri});
            }
            return results;
        }

        vm.destination_options = { suggest: suggest_paths };

        function suggest_contextIds(term) {
            var q = term.toLowerCase().trim();
            var results = [];

            // Find first 10 states that start with `term`.
            for (var i = 0; i < vm.contextIds.length && results.length < 10; i++) {
                var contextId = vm.contextIds[i];
                if (contextId.toLowerCase().indexOf(q) === 0)
                    results.push({label: contextId, value: contextId});
            }
            return results;
        }

        vm.contextId_options = { suggest: suggest_contextIds };

        function populateAutocomplete() {
            var queryTask = getUrlsQuery();
            OperationIndexService.postQuery(queryTask).then(function (response) {
                var nextPageLink = response.data.results.nextPageLink;
                if(nextPageLink !== "" && nextPageLink !== undefined) {
                    OperationIndexService.getPaginatedResults(nextPageLink).then(function (response) {
                        for (var i = 0; i < response.data.results.documentLinks.length; i++) {
                            var item = response.data.results.documentLinks[i];
                            if (vm.referrerUris.indexOf(response.data.results.documents[item].referer) === -1) {
                                vm.referrerUris.push(response.data.results.documents[item].referer);
                            }
                            if (vm.hostsList.indexOf(response.data.results.documents[item].host) === -1
                                && response.data.results.documents[item].host !== undefined) {
                                vm.hostsList.push(response.data.results.documents[item].host);
                            }
                            if (vm.portsList.indexOf(response.data.results.documents[item].port) === -1
                                &&response.data.results.documents[item].port !== -1) {
                                vm.portsList.push(response.data.results.documents[item].port);
                            }
                            if (vm.destinationPaths.indexOf(response.data.results.documents[item].path) === -1) {
                                vm.destinationPaths.push(response.data.results.documents[item].path);
                            }
                            if (vm.contextIds.indexOf(response.data.results.documents[item].contextId) === -1) {
                                vm.contextIds.push(response.data.results.documents[item].contextId);
                            }
                        }
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    });
                } else {
                    $scope.notification.type = "info";
                    $scope.notification.text = "There are no indexed operations";
                }
            }, function (error) {
                $scope.notification.type = "danger";
                $scope.notification.text = error.data.message;
            });

        }

        function getUrlsQuery() {
            //TODO: Add DESC sorting of documentUpdatedMicros in query once sorting is available
            var queryTask = {
                "taskInfo": {
                    "isDirect": true
                },
                "querySpec": {
                    "options": ["EXPAND_CONTENT"],
                    "resultLimit" : 1000,
                    "query": {
                        "term": {
                            "matchValue": "com:vmware:dcp:common:Operation:SerializedOperation",
                            "propertyName": "documentKind"
                        }
                    }
                },
                "indexLink": "/core/operation-index"
            };
            return queryTask;
        }

        /*******************GET AND DISPLAY RESULTS************************/
        function constructFilterQuery() {
            var terms = [];

            if (vm.queryOptions.referer.value !== '' && vm.queryOptions.referer.value !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.referer.value,
                        "propertyName": "referer"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.host.value !== '' && vm.queryOptions.host.value !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.host.value,
                        "propertyName": "host"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.port.value !== '' && vm.queryOptions.port.value !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.port.value,
                        "propertyName": "port"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.path.value !== '' && vm.queryOptions.path.value !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.path.value,
                        "propertyName": "path"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.action !== '' && vm.queryOptions.action !== undefined
                && vm.queryOptions.action !== null) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.action,
                        "propertyName": "action"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.jsonBody !== '' && vm.queryOptions.jsonBody !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.jsonBody + "*",
                        "matchType": "WILDCARD",
                        "propertyName": "jsonBody"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            if (vm.queryOptions.contextId.value !== '' &&
                vm.queryOptions.contextId.value !== undefined) {
                terms.push({
                    "term": {
                        "matchValue": vm.queryOptions.contextId.value,
                        "matchType": "WILDCARD",
                        "propertyName": "contextId"
                    },
                    "occurance": "MUST_OCCUR"
                });
            }

            var timeRangeOption = {
                "term": {
                    "range": {
                        "precisionStep": 1000000000,
                        "isMaxInclusive": "true",
                        "isMinInclusive": "true",
                        "max": "",
                        "min": "",
                        "type": "LONG"
                    },
                    "matchType": "TERM",
                    "propertyName": "documentUpdateTimeMicros"
                }
            };

            if (vm.queryOptions.fromTime !== '' && vm.queryOptions.fromTime !== undefined) {
                timeRangeOption.term.range.min = UtilService.convertDateToEpoch(vm.queryOptions.fromTime);
            }

            if (vm.queryOptions.toTime !== '' && vm.queryOptions.toTime !== undefined) {
                timeRangeOption.term.range.max = UtilService.convertDateToEpoch(vm.queryOptions.toTime);
            }

            if (timeRangeOption.term.range.max !== "" || timeRangeOption.term.range.min !== "") {
                terms.push(timeRangeOption);
            }

            var queryExpression = "";

            if (terms.length < 2) {
                queryExpression = terms[0];
            } else {
                queryExpression = {
                    "booleanClauses": []
                };
                for (var i = 0; i < terms.length; i++) {
                    queryExpression.booleanClauses[i] = terms[i];
                }
            }

            var query = {
                "taskInfo": {
                    "isDirect": true
                },
                "querySpec": {
                    "options": ["EXPAND_CONTENT"],
                    "resultLimit" : "1000",
                    "query": queryExpression
                },
                "indexLink": "/core/operation-index"
            };
            return query;
        };

        vm.getOperationsList = function () {
            if (validateFilter()) {
                OperationIndexService.getOperationsIndex(constructFilterQuery()).then(function (response) {
                        var nextPageLink = response.data.results.nextPageLink;
                        if (nextPageLink !== "" && nextPageLink !== undefined) {
                            OperationIndexService.getPaginatedResults(nextPageLink).then(function (response) {
                                vm.operationsList = response.data.results;
                                vm.graphOptions = {
                                    "lanes": [],
                                    "items": [],
                                    "laneLength": 0
                                };

                                if (vm.queryOptions.fromTime !== "") {
                                    vm.graphOptions.timeBegin = vm.queryOptions.fromTime;
                                }
                                if (vm.queryOptions.toTime !== "") {
                                    vm.graphOptions.timeEnd = vm.queryOptions.toTime;
                                }

                                for (var i in vm.operationsList.documents) {
                                    var referrerObj = vm.operationsList.documents[i];

                                    // get unique referrers
                                    if (vm.graphOptions.lanes.indexOf(referrerObj.referer) === -1) {
                                        vm.graphOptions.lanes.push(referrerObj.referer);

                                        // get all items per lane
                                        for (var j in vm.operationsList.documents) {
                                            if (vm.operationsList.documents[j].referer === referrerObj.referer) {
                                                vm.graphOptions.items.push({
                                                    "lane": vm.graphOptions.lanes.length - 1,
                                                    "id": j,
                                                    'action': referrerObj.action,
                                                    'jsonBody': referrerObj.jsonBody,
                                                    'documentKind': referrerObj.documentKind,
                                                    'referer': referrerObj.referer,
                                                    'path': referrerObj.path,
                                                    'port': referrerObj.port,
                                                    'host': referrerObj.host,
                                                    'contextId': referrerObj.contextId,
                                                    "start": Math.floor(referrerObj.documentUpdateTimeMicros / 1000)
                                                });

                                                // if item in graph, delete from list of available records
                                                delete vm.operationsList[j];
                                            }
                                        }
                                    }
                                }
                                vm.graphOptions.laneLength = vm.graphOptions.lanes.length;
                            }, function (error) {
                                $scope.notification.type = "danger";
                                $scope.notification.text = error.data.message;
                            });
                        } else {
                            $scope.notification.type = "info";
                            $scope.notification.text = "There are no indexed operations";
                        }
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.message;
                    }
                );
            } else {
                $scope.notification.type = "warning";
                $scope.notification.text = "Select at least one option in order to trace!";
            }
        };

        function validateFilter() {
            if ((vm.queryOptions.referer === '' || vm.queryOptions.referer === undefined) &&
                (vm.queryOptions.host === '' || vm.queryOptions.host === undefined) &&
                (vm.queryOptions.port === '' || vm.queryOptions.port === undefined) &&
                (vm.queryOptions.path === '' || vm.queryOptions.path === undefined) &&
                (vm.queryOptions.action === '' || vm.queryOptions.action === undefined ||
                vm.queryOptions.action === null) &&
                (vm.queryOptions.contextId === '' || vm.queryOptions.contextId === undefined) &&
                (vm.queryOptions.jsonBody === '' || vm.queryOptions.jsonBody === undefined) &&
                (vm.queryOptions.fromTime === '' || vm.queryOptions.fromTime === undefined) &&
                (vm.queryOptions.toTime === '' || vm.queryOptions.toTime === undefined)) {
                return false;
            }
            return true;
        }

        vm.startTracing = function () {
            OperationIndexService.startService().then(function (response) {
                vm.serviceStarted = true;
            }, function (error) {
                if(error.data.statusCode === 500) {
                    $scope.notification.type = "danger";
                    vm.serviceStarted = false;
                } else {
                    populateAutocomplete();
                    vm.serviceStarted = true;
                    $scope.notification.type = "info";
                }
                $scope.notification.text = error.data.message;
            });
        };

        vm.stopTracing = function () {
            OperationIndexService.stopService().then(function (response) {
                vm.serviceStarted = false;
                console.log(vm.serviceStarted);
            }, function (error) {
                if(error.status === 304 || error.data.statusCode === 400){
                    vm.serviceStarted = false;
                    $scope.notification.type = "info";
                } else {
                    vm.serviceStarted = true;
                    $scope.notification.type = "danger";
                }
                $scope.notification.text = error.data.message;
            });
        };
    }]);