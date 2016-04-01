/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
 */

'use strict';

(function() {

    /**
     * Pagination module for the xenon UI based on bootstrap css styles.
     * Paginate directive in this module paginates the list of data in the client side
     * based on the provided items per page configuration.
     * The paginate directive will not be visible unless more than one page is available.
     *
     * Usage:
     * Traditional usage to render all the elements:
     * <div ng-if="ctrl.documents.length > 0">
     *   <table class="table table-striped">
     *     <tr ng-repeat="result in ctrl.documents">
     *       <td><a ng-href="{{baseUrl + uiBase + ctrl.documents[$index]}}/instanceHome">
     *           {{ctrl.documents[$index]}}</a>
     *       </td>
     *     </tr>
     *   </table>
     * </div>
     *
     * Usage with xenon.ui.pagination - paginator directive
     * <div ng-if="ctrl.documents.length > 0">
     *   <paginator model="ctrl.documents" items-per-page="5"> <!-- paginator directive -->
     *     <table class="table table-striped">
     *       <tr ng-repeat="result in pageData"> <!-- Have to loop through the pageData instead of ctrl.documents -->
     *         <td><a ng-href="{{baseUrl + uiBase + pageData[$index]}}/instanceHome">
     *             {{pageData[$index]}}</a>
     *         </td>
     *       </tr>
     *     </table>
     *   </paginator>
     * </div>
     *
     * Paginator directive configurable properties:
     * model - model object to be rendered in pages
     * items-per-page - number of items to be displayed per page (default is 10)
     * properties - any additional properties/scope variables that needs to be accessed from the parent scope
     *              should be passed here as an json object. This keeps the pagination module independent of
     *              parent scope variables
     *
     * By default, the paginator displays 5 pages. If there are more than 5 pages, the paginator
     * displays the overflow ellipses on both the left side and right side to enable easy navigation
     * Eg for paginator overflow display, ... | 3 | 4 | 5 | ...
     *
     */
    angular.module("xenon.ui.pagination", [])

        /**
         * Paginator service (delegate) for some of the common operations to get the first page,
         * last page, prev page, next page and getting the list of pages to be rendered etc..
         */
        .factory("paginatorService", function() {

            return {
                //to get the new instance of the pagination service
                //pagination service stores the internal state of each
                //of the paginate directive, it needs a new instance for each paginate
                //directive
                getInstance: function(){
                    return new serviceInstance();
                }
            };

            /**
             * Internal service instance that maintains the state of the paginate control
             * All the internal states like currentPageNo, starting page, ending page etc
             * will be maintained here
             */
            function serviceInstance() {
                var totalPages,
                    startingPage = 1,
                    endingPage,
                    currentPage = startingPage,
                    DEFAULT_ITEMS_PER_PAGE = 10,
                    DEFAULT_NO_OF_PAGES = 5,
                    noOfPages = DEFAULT_NO_OF_PAGES,
                    itemsPerPage = DEFAULT_ITEMS_PER_PAGE,
                    data,
                    PAGE_OVERFLOW_PREFIX = '...',
                    PAGE_OVERFLOW_SUFFIX = PAGE_OVERFLOW_PREFIX,
                    needPrefixBit = 0,
                    needSuffixBit = 0,
                    service = this;


                /**
                 * Initializes the paginator service
                 * @param model
                 * @param noOfItemsPerPage
                 * @returns {*}
                 */
                service.init = function (model, noOfItemsPerPage) {
                    var totalItems = model.length;
                    if (noOfItemsPerPage) {
                        itemsPerPage = noOfItemsPerPage;
                    }
                    data = model;

                    //reset the starting only if it crosses the no of pages
                    startingPage = endingPage && (endingPage + needPrefixBit + needSuffixBit) >
                    noOfPages ? currentPage : startingPage;

                    totalPages = parseInt(totalItems / itemsPerPage) + ((totalItems % itemsPerPage) > 0 ? 1 : 0);
                };

                /**
                 * Gets the list of pages to be rendered
                 * @returns {Array}
                 */
                service.getPages = function () {
                    var pages = [],
                        me = this;
                    endingPage = totalPages < (startingPage + noOfPages - 1) ? totalPages
                        : (startingPage + noOfPages - 1);

                    //check if overflow prefix ellipses "..." is required
                    // if yes, create the prefix overflow obj
                    if (needPrefix()) {
                        needPrefixBit = 1;
                        var obj = createOverflowPageObj(PAGE_OVERFLOW_PREFIX);
                        obj.overflowPrefix = true;
                        pages.push(obj);
                    }
                    else {
                        needPrefixBit = 0;
                    }

                    //check if overflow suffix ellipses "..." is required
                    needSuffixBit = needSuffix() ? 1 : 0;

                    //after setting the prefix and suffix overflow ellipses,
                    // checking if the endingpage index needs to be changed
                    endingPage = me.recalculateEndingPage(startingPage, endingPage, needPrefixBit, needSuffixBit);

                    for (var i = startingPage; i <= endingPage; i++) {
                        var obj = {};
                        obj.pageNo = i;
                        obj.active = i == currentPage ? true : false;
                        pages.push(obj);
                    }

                    // if suffix is required, create the suffix overflow obj
                    if (needSuffixBit == 1) {
                        var obj = createOverflowPageObj(PAGE_OVERFLOW_SUFFIX);
                        obj.overflowSuffix = true;
                        pages.push(obj);
                    }

                    //creates the overflow page obj
                    function createOverflowPageObj(overflowString) {
                        var obj = {};
                        obj.pageNo = overflowString;
                        obj.active = false;
                        return obj;
                    }

                    //checks if prefix "..." is needed
                    function needPrefix() {
                        return startingPage > 1 && (endingPage - startingPage + 1 + needPrefixBit
                            + needSuffixBit) >= (noOfPages);
                    }

                    //checks if suffix "..." is needed
                    function needSuffix() {
                        return (endingPage < totalPages) ||
                            ((endingPage - startingPage + 1 + needPrefixBit) > noOfPages);
                    }

                    return pages;
                };

                /**
                 * recalculate the ending page if required
                 * @param startingPage
                 * @param endingPage
                 * @param needPrefixBit
                 * @param needSuffixBit
                 * @returns {*}
                 */
                service.recalculateEndingPage = function (startingPage, endingPage, needPrefixBit, needSuffixBit) {
                    var diff = (endingPage - startingPage + 1) + needPrefixBit + needSuffixBit - noOfPages;
                    if (diff > 0) {
                        endingPage = endingPage - diff;
                    }
                    return totalPages < endingPage ? totalPages : endingPage;
                };

                /**
                 * Gets the current page data
                 * @returns {Array}
                 */
                service.getCurrentPageData = function () {
                    var startIndex = parseInt(itemsPerPage * (currentPage - 1)),
                        endIndex = data.length < (startIndex + itemsPerPage) ? data.length :
                            (startIndex + itemsPerPage),
                        pageItems = [];

                    for (var i = startIndex; i < endIndex; i++) {
                        pageItems.push(data[i]);
                    }
                    return pageItems;
                };

                /**
                 * Gets the next page data
                 * @returns {Array}
                 */
                service.getNextPage = function () {
                    currentPage++;
                    if (endingPage != totalPages) {
                        startingPage++;
                    }
                    return this.getCurrentPageData();
                };

                /**
                 * Gets the previous page data
                 * @returns {Array}
                 */
                service.getPrevPage = function () {
                    currentPage--;
                    if (currentPage < startingPage) {
                        startingPage--;
                    }
                    return this.getCurrentPageData();
                };

                /**
                 * Gets the first page data
                 * @returns {Array}
                 */
                service.getFirstPage = function () {
                    currentPage = 1;
                    startingPage = 1;
                    return this.getCurrentPageData();
                };

                /**
                 * Gets the last page data
                 * @returns {Array}
                 */
                service.getLastPage = function () {
                    currentPage = totalPages;
                    startingPage = (totalPages - noOfPages + (totalPages > noOfPages ? 2 : 0));
                    return this.getCurrentPageData();
                };

                /**
                 * Gets the current page no
                 * @returns {number}
                 */
                service.getCurrentPageNo = function () {
                    return currentPage;
                };

                //Sets the current page no
                service.setCurrentPageNo = function (page) {
                    currentPage = page;
                };

                /**
                 * Gets the starting page no
                 * @returns {number}
                 */
                service.getStartingPageNo = function () {
                    return startingPage;
                };

                /**
                 * Sets the starting page no
                 */
                service.setStartingPageNo = function (page) {
                    startingPage = page;
                };

                /**
                 * Gets the ending page no
                 * @returns {number}
                 */
                service.getEndingPageNo = function () {
                    return endingPage;
                };

                /**
                 * Gets the total no of pages
                 * @returns {number}
                 */
                service.getTotalPages = function () {
                    return totalPages;
                };
            }

        })
        /**
         * Controller object to expose the important actions like getting next page,
         * prev page, first page and last page
         * @param $scope The scope object
         */
        .controller('paginatorController', ['$scope', function($scope){
            var PAGE_OVERFLOW_ELLIPSES = "...",
                ELLIPSES_PAGE_DIFF = 2,
                DISABLED = "disabled",
                MODEL = "model",
                vm = this;

            // Watch the model for any change and re-render the pagination control
            $scope.$watch(MODEL, function(newVal, oldVal) {
                var modelObj = newVal;
                if (!modelObj) {
                    return;
                }

                //Set the visibility of the paginator
                setVisibilityForPaginator(modelObj);
                //init paginator service
                vm.paginatorService.init(modelObj, $scope.itemsPerPage);

                //get pageData from model, pageData is set to the scope so that it will be easy
                // for the consumers to use it directly
                $scope.pageData = vm.paginatorService.getCurrentPageData();

                //get the Pages and re-render it
                vm.pages = vm.paginatorService.getPages();
            });

            /**
             * Called when the user clicks on any of the pages
             * @param page
             * @returns {*}
             */
            vm.selectPage = function(page) {
                //check if the page has ellipses overflow
                if (page.pageNo === PAGE_OVERFLOW_ELLIPSES) {
                    //change page nos, select the starting page no...
                    if (page.overflowSuffix) {
                        var startingPage = (vm.paginatorService.getStartingPageNo() + ELLIPSES_PAGE_DIFF);
                        vm.paginatorService.setStartingPageNo(startingPage);
                        vm.paginatorService.setCurrentPageNo(startingPage);
                    }
                    else if (page.overflowPrefix) {
                        var DEFAULT_STARTING_PAGE = 1,
                            startingPage = (vm.paginatorService.getStartingPageNo() - ELLIPSES_PAGE_DIFF) <= 0 ?
                                DEFAULT_STARTING_PAGE : (vm.paginatorService.getStartingPageNo() - ELLIPSES_PAGE_DIFF);
                        vm.paginatorService.setStartingPageNo(startingPage);
                        vm.paginatorService.setCurrentPageNo(startingPage);
                    }
                    $scope.pageData = vm.paginatorService.getCurrentPageData();
                }
                else {
                    vm.paginatorService.setCurrentPageNo(page.pageNo);
                    $scope.pageData = vm.paginatorService.getCurrentPageData();
                }
                vm.pages = vm.paginatorService.getPages();
            };

            /**
             * selects the first page
             * @returns {*}
             */
            vm.selectFirstPage = function() {
                if (!vm.paginatorService || vm.isFirstPageEnabled() === DISABLED) {
                    return;
                }
                $scope.pageData = vm.paginatorService.getFirstPage();
                vm.pages = vm.paginatorService.getPages();
            };

            /**
             * selects the last page
             * @returns {*}
             */
            vm.selectLastPage = function() {
                if (!vm.paginatorService || vm.isLastPageEnabled() === DISABLED) {
                    return;
                }
                $scope.pageData = vm.paginatorService.getLastPage();
                vm.pages = vm.paginatorService.getPages();
            };

            /**
             * selects the next page from the current context
             * @returns {*}
             */
            vm.selectNextPage = function() {
                if (!vm.paginatorService || vm.isNextPageEnabled() === DISABLED) {
                    return;
                }
                $scope.pageData = vm.paginatorService.getNextPage();
                vm.pages = vm.paginatorService.getPages();
            };

            /**
             * selects the previous page from the current context
             * @returns {*}
             */
            vm.selectPrevPage = function() {
                if (!vm.paginatorService || vm.isPrevPageEnabled() === DISABLED) {
                    return;
                }
                $scope.pageData = vm.paginatorService.getPrevPage();
                vm.pages = vm.paginatorService.getPages();
            };

            /**
             * checks to enable/disable first page and prev page buttons
             * @returns {String}
             */
            vm.isFirstPageEnabled = function() {
                if (vm.paginatorService && vm.paginatorService.getStartingPageNo() == 1) {
                    return DISABLED;
                }

            };

            /**
             * checks to enable/disable last page button
             * @returns {String}
             */
            vm.isLastPageEnabled = function() {
                if (vm.paginatorService &&
                    vm.paginatorService.getEndingPageNo() == vm.paginatorService.getTotalPages()) {
                    return DISABLED;
                }
            };

            /**
             * checks to enable/disable next page button
             * @returns {String}
             */
            vm.isNextPageEnabled = function() {
                if (vm.paginatorService &&
                    vm.paginatorService.getCurrentPageNo() == vm.paginatorService.getTotalPages()) {
                    return DISABLED;
                }

            };

            /**
             * checks to enable/disable next page button
             * @returns {String}
             */
            vm.isPrevPageEnabled = function() {
                if (vm.paginatorService && vm.paginatorService.getCurrentPageNo() == 1) {
                    return DISABLED;
                }

            };

            //.................................Private methods..........................//

            //Sets the visibility for the paginator
            function setVisibilityForPaginator(model) {
                if (model && model.length > $scope.itemsPerPage) {
                    $scope.hidePaginator = false;
                }
                else {
                    $scope.hidePaginator = true;
                }
            }
        }])
        /**
         * Paginator directive for handling the pagination
         * Usage:
         *   <paginator model="ctrl.documents" items-per-page="5"> <!-- paginator directive -->
         *     <table class="table table-striped">
         *       <tr ng-repeat="result in pageData"> <!-- Have to loop through the pageData instead of ctrl.documents -->
         *         <td><a ng-href="{{baseUrl + uiBase + pageData[$index]}}/instanceHome">
         *             {{pageData[$index]}}</a>
         *         </td>
         *       </tr>
         *     </table>
         *   </paginator>
         *
         *   Directive attributes:
         *   model - The actual model to be rendered as pages
         *   itemsPerPage - No of items to be rendered per page
         *   properties - any additional properties/scope variables that needs to be accessed from the parent scope
         *                should be passed here as an json object. This keeps the pagination module independent of
         *                parent scope variables
         */
        .directive("paginator", ['paginatorService', function(paginatorService) {
            return {
                restrict: 'EA',
                transclude: true,
                templateUrl: "/user-interface/resources/com/vmware/xenon/ui/UiService/pagination/pagination.html",
                scope: {
                    model: "=",
                    itemsPerPage: "=",
                    properties: "="
                },
                link: function($scope, element, attributes, controller, transclude) {
                    //constants
                    var PAGINATION = 'pagination',
                        VISIBILITY = 'visibility';

                    //setting the initial variables to controller
                    controller.paginatorService = paginatorService.getInstance();
                    //setting the scope to hide the paginator initially and unhide it
                    //when there are more than one page
                    $scope.hidePaginator = true;

                    //hide the paginator until the no of pages exceeds min number 1
                    $scope.$watch("hidePaginator", function(newVal, oldVal) {
                        if (newVal) {
                            findChild(element, PAGINATION).css(VISIBILITY, 'hidden');
                        }
                        else {
                            findChild(element, PAGINATION).css(VISIBILITY, 'visible');
                        }
                    });

                    //find the child element to hide/unhide
                    function findChild(element, id) {
                        var children = element.children();
                        for (var i = 0, len = children.length; i < len; i++) {
                            if (children[i].className.indexOf(id) != -1) {
                                return angular.element(children[i]);
                            }
                        }
                    }

                },
                controller: 'paginatorController',
                controllerAs: 'vm'
            }
        }])
        //Helper directive to overcome the problem of transclusion when using directive template url
        .directive('inject', function() {
            return {
                link: function($scope, $element, $attrs, controller, $transclude) {
                    if (!$transclude) {
                        throw minErr('ngTransclude')('orphan',
                            'Illegal use of ngTransclude directive '+
                            'Element: {0}',
                            startingTag($element));
                    }
                    var innerScope = $scope.$new();
                    $transclude(innerScope, function(clone) {
                        $element.empty();
                        $element.append(clone);
                        $element.on('$destroy', function() {
                            innerScope.$destroy();
                        });
                    });
                }
            };
        });

})();