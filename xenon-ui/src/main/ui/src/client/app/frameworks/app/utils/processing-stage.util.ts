// import {ProcessingStage} from '../../../frameworks/app/enums/index';
import {DisplayInfo} from '../../../frameworks/app/interfaces/index';

export class ProcessingStageUtil {
    static UNKNOWN: DisplayInfo = {
        className: 'created-status',
        iconClassName: 'fa fa-ban',
        displayName: 'Unknown'
    };

    static UNAVAILABLE: DisplayInfo = {
        className: 'created-status',
        iconClassName: 'fa fa-ban',
        displayName: 'Unknown'
    };

    // Execution states
    static CREATED: DisplayInfo = {
        className: 'created-status',
        iconClassName: 'fa fa-dot-circle-o',
        displayName: 'Created'
    };

    static REPLACED: DisplayInfo = {
        className: 'created-status',
        iconClassName: 'fa fa-random',
        displayName: 'Replaced'
    };

    static INITIALIZING: DisplayInfo = {
        className: 'created-status',
        iconClassName: 'fa fa-dot-circle-o',
        displayName: 'Initializing'
    };

    static LOADING_INITIAL_STATE: DisplayInfo = {
        className: 'execute-status',
        iconClassName: 'fa fa-spinner fa-pulse',
        displayName: 'Loading Initial State'
    };

    static SYNCHRONIZING: DisplayInfo = {
        className: 'execute-status',
        iconClassName: 'fa fa-refresh fa-spin',
        displayName: 'Synchronizing'
    };

    static EXECUTING_CREATE_HANDLER: DisplayInfo = {
        className: 'execute-status',
        iconClassName: 'fa fa-cog fa-spin',
        displayName: 'Executing: Create Handler'
    };

    static EXECUTING_START_HANDLER: DisplayInfo = {
        className: 'execute-status',
        iconClassName: 'fa fa-cog fa-spin',
        displayName: 'Executing: Start Handler'
    };

    static INDEXING_INITIAL_STATE: DisplayInfo = {
        className: 'execute-status',
        iconClassName: 'fa fa-cog fa-spin',
        displayName: 'Indexing: Initial State'
    };

    static AVAILABLE: DisplayInfo = {
        className: 'finished-status',
        iconClassName: 'fa fa-check-circle-o',
        displayName: 'Available'
    };

    static PAUSED: DisplayInfo = {
        className: 'skipped-status',
        iconClassName: 'fa fa-pause-circle-o',
        displayName: 'Paused'
    };

    static STOPPED: DisplayInfo = {
        className: 'failed-status',
        iconClassName: 'fa fa-dot-circle-o',
        displayName: 'Stopped'
    };
}
