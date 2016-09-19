// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../core/index';

import { NotificationTypeUtil } from './notification-type.util';

import { Notification } from '../../interfaces/index';
import { NotificationService } from '../../services/index';

@BaseComponent({
    selector: 'xe-notification',
    moduleId: module.id,
    templateUrl: './notification.component.html',
    styleUrls: ['./notification.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class NotificationComponent implements OnInit, OnDestroy {
    /**
     * One or more notifications that need to be displayed.
     */
    private _notifications: Notification[] = [];

    /**
     * Subscriptions to services.
     */
    private _notificationServiceSubscription: Subscription;

    constructor(private _notificationService: NotificationService) {}

    ngOnInit(): void {
        this._notificationServiceSubscription = this._notificationService.get().subscribe(
            (data) => {
                this._notifications = data;
            });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._notificationServiceSubscription)) {
            this._notificationServiceSubscription.unsubscribe();
        }
    }

    getNotifications(): Notification[] {
        return this._notifications;
    }

    getNotificationTypeClass(notificationType: string): string {
        return NotificationTypeUtil.getNotificationType(notificationType).className;
    }
}
