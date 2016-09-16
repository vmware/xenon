import * as _ from 'lodash';

import {DisplayInfo} from '../../../frameworks/app/interfaces/index';

export class ProcessLogTypeUtil {
    static UNKNOWN: DisplayInfo = {
        className: 'unknown',
        iconClassName: '',
        displayName: 'Unknown'
    };

    static INFO: DisplayInfo = {
        className: 'info',
        iconClassName: 'fa fa-info',
        displayName: 'INFO'
    };

    static WARNING: DisplayInfo = {
        className: 'warning',
        iconClassName: 'fa fa-exclamation-triangle',
        displayName: 'WARNING'
    };

    static SEVERE: DisplayInfo = {
        className: 'severe',
        iconClassName: 'fa fa-exclamation-circle',
        displayName: 'SEVERE'
    };

    /**
     * Return the display info for the given log type string
     */
    static getLogType(logType: string): DisplayInfo {
        if (!logType || _.isUndefined(this[logType])) {
            return this.UNKNOWN;
        }

        return this[logType];
    }
}
