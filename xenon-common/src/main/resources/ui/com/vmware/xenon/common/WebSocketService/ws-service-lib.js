/*
 * WebSocket-based ephemeral DCP service library. Allows to plug in services with in-browser JavaScript implementation
 * into DCP node. These services are fully functional services which may interact with other services and may be
 * interacted with. Here is an example implementation of trivial observer service:
 *
 * new WSL.WebSocketConnection("/core/ws-endpoint").createService(
 *   function (op) { // Request handler
 *     alert("Example document created: " + op.body.documentSelfLink);
 *     op.complete();
 *   },
 *   function (wss) { // OnServiceCreated handler
 *     wss.subscribe("/core/examples/subscriptions");
 *   }
 * );
 *
 */
var WSL = (function () {
    var CR_LF = "\r\n";
    var DELETE = "DELETE";
    var POST = "POST";
    var REPLY = "REPLY";
    var UNDEFINED = "undefined";
    var WS_SERVICE_PATH = "/ws-service";

    /*
     An asynchronous operation. Receiver of this operation should eventually call complete() or one of fail() methods
     in order to notify caller that the operation is complete.
     */
    function Operation() {
        this.action = null;
        this.statusCode = 200;
        this.body = null;
        this.id = 0;

        this.oncompletion = function () {
        };

        this.complete = function () {
            this.oncompletion();
        };

        this.fail = function (exception) {
            this.statusCode = 500;
            this.body = exception;
            this.complete();
        };

        this.fail = function (exception, body) {
            this.statusCode = 500;
            this.body = body;
            this.complete();
        };
    }

    /*
     This class is main entry point for the library. Upon creation it establishes a websocket connection to the node.
     Within a connection zero or more ephemeral services may me created using createService method. These services
     are accessible from anywhere in the DCP cluster and live as long WebSocket connection lives.

     For observer subscription on other services, ephemeral websocket services should use service subscribe()/
     unsubscribe() method instead of directly POSTing/DELETEing at service/subscriptions URL. That would allow
     server node to delete service subscription in case when a service is gone (for example when user closed
     browser tab).
     */
    function WebSocketConnection(endpoint) {
        var me = this;
        this.ready = false;
        this.webSocket = new WebSocket(getWebsocketUri(location, endpoint));
        this.services = {};
        this.doneHandlers = {};
        this.requestCounter = 0;
        this.requestQueue = [];
        this.failure = null;

        this.webSocket.onerror = function (event) {
            me.failure = "Failed to establish a websocket connection to " + endpoint;
            if (!me.ready) {
                me.ready = true;
                for (var i = 0; i < me.requestQueue.length; i++) {
                    var req = me.requestQueue[i];
                    doRequest(me, req.method, req.path, req.body, req.doneHandler);
                }
                me.requestQueue = [];
            }
        };

        this.webSocket.onopen = function (event) {
            me.webSocket.onmessage = function (ev) {
                var crlf, body;
                if (ev.data.indexOf(POST) == 0) {
                    crlf = ev.data.indexOf(CR_LF);
                    if (crlf < 0) {
                        crlf = ev.data.length;
                    }
                    var serviceUri = ev.data.substr(5, crlf - 5);
                    body = crlf == ev.data.length ? null : ev.data.substr(crlf + 2);
                    var sop = JSON.parse(body);
                    if (me.services.hasOwnProperty(serviceUri)) {
                        var op = new Operation();
                        var service = me.services[serviceUri];
                        op.action = sop.action;
                        op.id = sop.id;
                        op.body = JSON.parse(sop.jsonBody);
                        op.oncompletion = function () {
                            var response = {
                                "id": op.id,
                                "responseHeaders": {},
                                "statusCode": op.statusCode,
                                "responseJsonBody": JSON.stringify(op.body)
                            };
                            doRequest(me, REPLY, getLocation(serviceUri), response, null);
                        };
                        service.handleRequest(op);
                    }
                } else {
                    var spaceIndex = ev.data.indexOf(" ");
                    crlf = ev.data.indexOf(CR_LF);
                    if (crlf < 0) {
                        crlf = ev.data.length;
                    }
                    if (spaceIndex < 0) {
                        return;
                    }
                    var statusCode = ev.data.substr(0, spaceIndex);
                    var requestId = ev.data.substr(spaceIndex + 1, crlf - spaceIndex - 1);
                    body = crlf == ev.data.length ? null : ev.data.substr(crlf + 2);
                    if (me.doneHandlers.hasOwnProperty(requestId)) {
                        me.doneHandlers[requestId](statusCode, body);
                        delete me.doneHandlers[requestId];
                    }
                }
            };
            if (!me.ready) {
                me.ready = true;
                for (var i = 0; i < me.requestQueue.length; i++) {
                    var req = me.requestQueue[i];
                    doRequest(me, req.method, req.path, req.body, req.doneHandler);
                }
                me.requestQueue = [];
            }
        };

        /*
         Creates an ephemeral service. handleRequest is used a request handler for new service and
         onServiceReady(service) or onFailure(http_status_code, body) is called when service operation succeed or
         fails.
         */
        this.createService = function (handleRequest, onServiceReady, onFailure) {
            doRequest(this, POST, WS_SERVICE_PATH, null, function (code, body) {
                if (code >= 200 && code < 300) {
                    var serviceUri = JSON.parse(body).uri;
                    var service = new WebSocketService(me, serviceUri, handleRequest);
                    me.services[serviceUri] = service;
                    onServiceReady(service);
                } else {
                    if (typeof onFailure !== UNDEFINED) {
                        onFailure(code, body);
                    }
                }
            });
        }
    }

    function WebSocketService(connection, selfLink, handleRequest) {
        this.connection = connection;
        this.uri = selfLink;
        this.handleRequest = handleRequest;

        /*
         Registers this service as an observer to the specifies service subscriptions URL. onSuccess() or
         onFailure(http_status_code, body) is called when operation is complete.
         */
        this.subscribe = function (serviceUri, onSuccess, onFailure) {
            doRequest(this.connection, POST, serviceUri, {reference: this.uri, replayState: true}, function (code, body) {
                if (code >= 200 && code < 300) {
                    if (typeof onSuccess !== UNDEFINED) {
                        onSuccess();
                    }
                } else {
                    if (typeof onFailure !== UNDEFINED) {
                        onFailure(code, body);
                    }
                }
            });
        };

        /*
         Un-registers this service as an observer to the specifies service subscriptions URL. onSuccess() or
         onFailure(http_status_code, body) is called when operation is complete.
         */
        this.unsubscribe = function (serviceUri, onSuccess, onFailure) {
            doRequest(this.connection, DELETE, serviceUri, {reference: this.uri}, function (code, body) {
                if (code >= 200 && code < 300) {
                    if (typeof onSuccess !== UNDEFINED) {
                        onSuccess();
                    }
                } else {
                    if (typeof onFailure !== UNDEFINED) {
                        onFailure(code, body);
                    }
                }
            });
        };

        /*
         Stops this service. onSuccess() or onFailure(http_status_code, body) is called when operation is complete.
         */
        this.stop = function (onSuccess, onFailure) {
            doRequest(this.connection, DELETE, getLocation(this.uri), null, function (code, body) {
                if (code >= 200 && code < 300) {
                    if (typeof onSuccess !== UNDEFINED) {
                        onSuccess();
                    }
                } else {
                    if (typeof onFailure !== UNDEFINED) {
                        onFailure(code, body);
                    }
                }
            });
        }
    }

    function doRequest(wsc, method, path, body, doneHandler) {
        if (!wsc.ready) {
            wsc.requestQueue.push({
                method: method,
                path: path,
                body: body,
                doneHandler: doneHandler
            });
            return;
        }
        if (wsc.failure !== null) {
            doneHandler(500, wsc.failure);
            return;
        }
        var requestId = wsc.requestCounter++;
        var request = requestId + CR_LF + method + " " + path;
        if (body != null) {
            request += CR_LF + JSON.stringify(body);
        }
        wsc.webSocket.send(request);
        if (doneHandler != null) {
            wsc.doneHandlers[requestId] = doneHandler;
        }
    }


    function getLocation(href) {
        var l = document.createElement("a");
        l.href = href;
        return l.pathname;
    }

    function getWebsocketUri(location, endpoint) {
        var proto = location.protocol.toLowerCase().indexOf("https") >= 0 ? "wss:" : "ws:";
        return proto + "//" + location.host + endpoint;
    }

    return {
        WebSocketConnection: WebSocketConnection
    }
}());