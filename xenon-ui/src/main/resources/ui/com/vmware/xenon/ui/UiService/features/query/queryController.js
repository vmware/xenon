/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

angular.module('dcpDefault').controller('QueryController', ['$scope', 'QueryService', 'UtilService',
        '$routeParams', '$timeout',

        function ($scope, QueryService, UtilService, $routeParams, $timeout) {
            //TODO: Add localization (tracker: 92459826)
            var vm = this;
            vm.query = {
                order: 'ASC',
                expand: ''
            };

            vm.querySpec = {};
            vm.simpleResults;
            vm.advancedResults;
            vm.queryingResults = false;
            vm.queryType = 'simple';

            vm.simpleQuery = [{
                "term": {
                    "propertyName": "",
                    "matchType": "",
                    "matchValue": ""
                }
            }];

            $scope.booleanClauses = [{
                "occurance": CONSTANTS.MUST_OCCUR,
                "booleanClauses": [{
                    "occurance": CONSTANTS.MUST_OCCUR,
                    "term": {
                        "propertyName": "",
                        "matchType": "",
                        "matchValue": ""
                    }
                }]
            }];

            vm.searchParameters = [{
                "id": CONSTANTS.DOC_KIND,
                "name": CONSTANTS.DOC_KIND,
                "type": CONSTANTS.DOC_KIND
            }];

            vm.booleanConnectors = [
                {id: CONSTANTS.MUST_OCCUR, name: "All conditions must be satisfied (AND)"},
                {
                    id: CONSTANTS.SHOULD_OCCUR,
                    name: "Any one of the conditions can be satisfied (OR)"
                },
                {
                    id: CONSTANTS.MUST_NOT_OCCUR,
                    name: "None of the conditions can be satisfied (NOT)"
                }
            ];

            /* TODO: Associate searchParameters with matchTypes and display appropriate matchtypes
             based on the type of searchParameter (tracker: 92461252)
             */
            vm.matchTypes = [
                {id: CONSTANTS.WILDCARD, name: "matches"},
                {id: CONSTANTS.PHRASE, name: "string is"},
                {id: CONSTANTS.TERM, name: "(text) is"}
            ];

            UtilService.getServiceTemplate($routeParams.path, $routeParams.serviceName).
                then(function (response) {
                    var docLink = response.data.documentLinks[0];
                    var properties = response.data.documents[docLink].documentDescription.
                        propertyDescriptions;
                    traverseJSON(properties, '');
                }, function (error) {
                    $scope.notification.type = "danger";
                    $scope.notification.text = error.data.message;
                });

            vm.setOrder = function (order) {
                vm.query.order = order;
            };

            vm.switchQueryView = function (value) {
                vm.queryType = value;
            };

            vm.addRule = function (siblingRule, position, occurance, childRule) {
                if (childRule == null) {
                    siblingRule.splice(position + 1, 0, {
                        "occurance": occurance,
                        "term": {
                            "propertyName": "",
                            "matchType": "",
                            "matchValue": ""
                        }
                    });
                } else {
                    parentRule.splice(position + 1, 0, childRule);
                }
            };

            vm.addNestedRule = function (parentRule, newRule) {
                if (newRule == null) {
                    parentRule.booleanClauses.push({
                        "occurance": CONSTANTS.MUST_OCCUR,
                        "booleanClauses": [{
                            "occurance": CONSTANTS.MUST_OCCUR,
                            "term": {
                                "propertyName": "",
                                "matchType": "",
                                "matchValue": ""
                            }
                        }]
                    });
                } else {
                    parentRule.booleanClauses.push(newRule);
                }
            };

            vm.deleteRule = function (data, index) {
                data.splice(index, 1);
            };

            vm.adjustChildren = function (parent) {
                for (var i = 0; i < parent.booleanClauses.length; i++) {
                    if (!parent.booleanClauses[i].booleanClauses) {
                        parent.booleanClauses[i].occurance = parent.occurance;
                    }
                }
            };

            vm.submitQuery = function () {
                clearResults();
                QueryService.postQuery($routeParams.path,
                    vm.constructQueryObject(vm.queryType === 'simple' ? vm.simpleQuery
                        : $scope.booleanClauses)).
                    then(function (response) {
                        vm.queryingResults = true;
                        var querySelfLink = response.data.documentSelfLink;

                        var pollResults = function (queryPath) {
                            $timeout(function () {
                                QueryService.getQueryResults(queryPath).then(function (response) {
                                    switch (response.data.taskInfo.stage) {
                                        case CONSTANTS.TASK_STAGE.FINISHED:
                                            setResults(response.data.results);
                                            break;
                                        case CONSTANTS.TASK_STAGE.CANCELLED:
                                            $scope.notification.type = "danger";
                                            $scope.notification.text = response.data.taskInfo.failure.message;
                                            break;
                                        case CONSTANTS.TASK_STAGE.FAILED:
                                            $scope.notification.type = "danger";
                                            $scope.notification.text = response.data.taskInfo.failure.message;
                                            break;
                                        default:
                                            pollResults(querySelfLink);
                                            break;
                                    }
                                }, function (error) {
                                    $scope.notification.type = "danger";
                                    $scope.notification.text = error.data.taskInfo.failure.message;
                                }).finally(function () {
                                    vm.queryingResults = false;
                                });
                            }, 1000);
                        };
                        pollResults(querySelfLink);
                    }, function (error) {
                        $scope.notification.type = "danger";
                        $scope.notification.text = error.data.taskInfo.failure.message;
                    });
            };

            vm.constructQueryObject = function (query) {
                var specObject = {
                    "querySpec": {
                        "query": query[0],
                        "sortTerm": {
                            "propertyName": "sortTerm",
                            "matchValue": vm.query.order
                        }
                    }
                };

                if (vm.query.expand) {
                    specObject.querySpec.options = [CONSTANTS.EXPAND_CONTENT];
                }
                return specObject;
            };

            var clearResults = function () {
                if (vm.queryType === 'simple') {
                    vm.simpleResults = "";
                } else {
                    vm.advancedResults = "";
                }
            };

            var setResults = function (results) {
                if (vm.queryType === 'simple') {
                    vm.simpleResults = results;
                } else {
                    vm.advancedResults = results;
                }
            };

            var traverseJSON = function (obj, parent) {
                for (var prop in obj) {
                    if (obj[prop].typeName == "PODO") {
                        traverseJSON(obj[prop].nestedDescription.propertyDescriptions,
                            parent + (parent ? '.' : '') + prop);
                    } else {
                        vm.searchParameters.push({
                            "id": parent + (parent ? '.' : '') + prop,
                            "name": parent + (parent ? '.' : '') + prop,
                            "type": obj[prop].typeName
                        });
                    }
                }
            };
        }]
);
