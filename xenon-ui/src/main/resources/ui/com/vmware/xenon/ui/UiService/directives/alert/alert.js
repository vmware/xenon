dcpDefault.directive("alert", function(){
    return{
        restrict: 'EA',
        templateUrl: "/user-interface/resources/com/vmware/dcp/ui/UiService/directives/alert/alert.html",
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