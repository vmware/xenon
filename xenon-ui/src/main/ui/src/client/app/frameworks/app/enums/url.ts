const API_PREFIX: string = String('<%= ENV %>') === 'prod' ? '' : '/api';

export class URL {
    static WEB_SOCKET: string = 'ws://';
    static API_PREFIX: string = API_PREFIX;
    static CONFIG_SUFFIX: string = '/config';
    static STATS_SUFFIX: string = '/stats';
    static FORWARDING_PATH: string = '/core/node-selectors/default/forwarding';

    static Root: string = '/';

    static Authentication: string = '/core/authn/basic';
    static CoreManagement: string = '/core/management';
    static NodeGroup: string = '/core/node-groups/';
    static Log: string = '/core/management/process-log';
}
