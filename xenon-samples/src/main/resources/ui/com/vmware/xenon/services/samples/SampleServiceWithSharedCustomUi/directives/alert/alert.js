customUiApp.directive("alert", function(){
    return{
        restrict: 'EA',
        templateUrl: "/user-interface/resources/com/vmware/xenon/services/samples/SampleServiceWithSharedCustomUi/directives/alert/alert.html",
        replace: false,
        transclude: false,
        scope: {
            message: "=",
            close: "&"
        },
        link: function(scope){
            scope.close = function() {
                scope.message = {};
            }
        }
    };
});