// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { NotificationTypeUtil } from './notification-type.util';
import { Notification } from '../../interfaces/index';
import { NotificationService } from '../../services/index';

@Component({
    selector: 'xe-notification',
    moduleId: module.id,
    templateUrl: './notification.component.html',
    styleUrls: ['./notification.component.css']
})

export class NotificationComponent implements OnInit, OnDestroy {
    /**
     * One or more notifications that need to be displayed.
     */
    private notifications: Notification[] = [];

    /**
     * Subscriptions to services.
     */
    private notificationServiceSubscription: Subscription;

    constructor(private notificationService: NotificationService) {}

    ngOnInit(): void {
        this.notificationServiceSubscription = this.notificationService.get().subscribe(
            (data) => {
                this.notifications = data;
            });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.notificationServiceSubscription)) {
            this.notificationServiceSubscription.unsubscribe();
        }
    }

    getNotifications(): Notification[] {
        return this.notifications;
    }

    getNotificationTypeClass(notificationType: string): string {
        return NotificationTypeUtil.getNotificationType(notificationType).className;
    }
}
