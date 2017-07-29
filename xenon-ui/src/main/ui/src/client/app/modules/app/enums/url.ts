const API_PREFIX: string = String('<%= BUILD_TYPE %>') === 'prod' ? '' : '/api';

export class URL {
    static WEB_SOCKET: string = 'ws://';
    static API_PREFIX: string = API_PREFIX;
    static CONFIG_SUFFIX: string = '/config';
    static STATS_SUFFIX: string = '/stats';
    static NODE_SELECTOR: string = '/core/node-selectors';

    // NOTE: Root need to be POSTed to with body
    // { "kind": "com:vmware:xenon:ui:UiService:QueryRequest" }
    static Root: string = '/core/ui/default';
    static RootPostBody: any = {
        kind: 'com:vmware:xenon:ui:UiService:QueryRequest'
    };

    static Authentication: string = '/core/authn/basic';
    static CoreManagement: string = '/core/management';
    static NodeGroup: string = '/core/node-groups/';
    static Log: string = '/core/management/process-log';
    static Query: string = '/core/query-tasks';
    static QueryLocal: string = '/core/local-query-tasks';
    static DocumentIndex: string = '/core/document-index';
}
