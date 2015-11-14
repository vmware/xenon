var wsEndpoint = "/core/ws-endpoint";
var exampleServiceSubscriptions = "/core/examples/subscriptions";

new WSL.WebSocketConnection(wsEndpoint).createService(
    function (op) {
        try {
            $("#objects-created").append('<a href="' + op.body.documentSelfLink + '">' + op.body.name +
                ' (' + op.body.documentSelfLink + ')</a><br/>');
        } finally {
            op.complete();
        }
    },
    function (wss) {
        wss.subscribe(exampleServiceSubscriptions);
    }
);
