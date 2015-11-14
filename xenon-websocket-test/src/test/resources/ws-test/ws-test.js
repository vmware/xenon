var examplesServiceLink = "/core/examples/subscriptions";

var connection = new WSL.WebSocketConnection("/core/ws-endpoint");
var observerService;
var observerServiceUri;
var echoServiceUri;
var objectsCreated = [];

connection.createService(
    function (op) {
        try {
            objectsCreated.push(op.body.documentSelfLink);
            op.complete();
        } catch (e) {
            op.fail(e);
        }
    },
    function (wss) {
        wss.subscribe(examplesServiceLink);
        observerService = wss;
        observerServiceUri = wss.uri;
    }
);

connection.createService(
    function (op) {
        try {
            op.body = {method: op.action, requestBody: op.body};
        } finally {
            op.complete();
        }
    },
    function (wss) {
        echoServiceUri = wss.uri;
    }
);
