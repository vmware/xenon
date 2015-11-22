var examplesServiceLink = "/core/examples/subscriptions";

var connection = new WSL.WebSocketConnection("/core/ws-endpoint");
var observerServiceForStop;
var observerServiceForClose;
var observerServiceForUnsubscribe;

var observerServiceUriForStop;
var observerServiceUriForClose;
var observerServiceUriForUnsubscribe;
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
        observerServiceForStop = wss;
        observerServiceUriForStop = wss.uri;
    }
);

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
        observerServiceForClose = wss;
        observerServiceUriForClose = wss.uri;
    }
);

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
        observerServiceForUnsubscribe = wss;
        observerServiceUriForUnsubscribe = wss.uri;
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
