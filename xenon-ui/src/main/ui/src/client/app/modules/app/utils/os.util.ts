import { DisplayInfo } from '../../../modules/app/interfaces/index';

export class OsUtil {
    static OTHER: DisplayInfo = {
        className: 'other',
        iconClassName: 'fa fa-asterisk',
        displayName: 'Other'
    };

    static WINDOWS: DisplayInfo = {
        className: 'windows',
        iconClassName: 'fa fa-windows',
        displayName: 'Windows'
    };

    static LINUX: DisplayInfo = {
        className: 'linux',
        iconClassName: 'fa fa-linux',
        displayName: 'Linux'
    };

    static MACOS: DisplayInfo = {
        className: 'mac',
        iconClassName: 'fa fa-apple',
        displayName: 'Mac OS'
    };
}
