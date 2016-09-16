import { NotificationType } from '../../enums/notification-type';
import { DisplayInfo } from '../../interfaces/index';

export class NotificationTypeUtil {
    static UNKNOWN: DisplayInfo = {
        className: '',
        iconClassName: '',
        displayName: 'Unknown'
    };

    static INFO: DisplayInfo = {
        className: 'alert-info',
        iconClassName: 'assets/images/ic_info_16x.svg',
        displayName: 'Info'
    };

    static WARNING: DisplayInfo = {
        className: 'alert-warning',
        iconClassName: 'assets/images/ic_warning_16x.svg',
        displayName: 'Warning'
    };

    static ERROR: DisplayInfo = {
        className: 'alert-danger',
        iconClassName: 'assets/images/ic_error_red_16x.svg',
        displayName: 'Error'
    };

    static SUCCESS: DisplayInfo = {
        className: 'alert-success',
        iconClassName: 'assets/images/ic_success_16x.svg',
        displayName: 'Success'
    };

    /**
     * Return the display info for the given notificationType
     */
    static getNotificationType(notificationType: string): DisplayInfo {
        if (NotificationType[notificationType] === NotificationType.INFO) {
            return this.INFO;
        }

        if (NotificationType[notificationType] === NotificationType.WARNING) {
            return this.WARNING;
        }

        if (NotificationType[notificationType] === NotificationType.ERROR) {
            return this.ERROR;
        }

        if (NotificationType[notificationType] === NotificationType.SUCCESS) {
            return this.SUCCESS;
        }

        return this.UNKNOWN;
    }

}
