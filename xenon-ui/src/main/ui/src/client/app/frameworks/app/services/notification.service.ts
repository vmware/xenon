import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { Notification } from '../interfaces/index';

@Injectable()
export class NotificationService {
    /**
     * The subject (both Observable and Observer) that monitors the Notifications.
     */
    private _notificationSubject: Subject<Notification[]> = new Subject<Notification[]>();

    set(notifications: Notification[]): void {
        this._notificationSubject.next(notifications);
    }

    get(): Observable<Notification[]> {
        return this._notificationSubject as Observable<Notification[]>;
    }
}
