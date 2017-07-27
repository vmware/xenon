// angular
import { Component, Input, SimpleChange, OnChanges } from '@angular/core';
import * as _ from 'lodash';

@Component({
    selector: 'xe-alert',
    moduleId: module.id,
    templateUrl: './alert.component.html',
    styleUrls: ['./alert.component.css']
})

export class AlertComponent implements OnChanges {
    /**
     * The messages to be displayed in the alert. It can be multiple messages
     * with the same type.
     */
    @Input()
    messages: any[];

    /**
     * Whether the alert is global or not. Global alerts are always placed first
     * on the page. They occupy the complete width of the parent container.
     */
    @Input()
    isGlobal: boolean = false;

    /**
     * Whether the alert is a small one. When display alerts in cards, use only
     * small alerts.
     */
    @Input()
    isSmall: boolean = false;

    /**
     * Type of the alert. It should be one of the four possible values:
     * 'alert-info', 'alert-warning', 'alert-danger' and 'alert-success'.
     * If not provided it will be 'alert-info' be default.
     */
    @Input()
    type: string = 'alert-info';

    /**
     * Whether the alert is closable or not.
     */
    @Input()
    closable: boolean = false;

    /**
     * Whether the alert will automatically be dismissed without requiring user
     * to click the close button. If set to true, the alert will dismiss after 4
     * seconds. Note that it will not dismiss if user put mouse on the alert, but
     * will restart the 4 seconds countdown as soon as user's mouse leaves
     * the alert.
     */
    @Input()
    autoDismiss: boolean = false;

    /**
     * Whether the alert is visible or not.
     */
    isVisible: boolean = false;

    /**
     * Keep track of the timeout object so it can be cleared or resumed depends
     * on user's interactions.
     */
    private autoDismissTimeout: any;

    /**
     * The default timeout delay. 4 seconds.
     */
    private defaultTimeoutDelay: number = 4000;

    /**
     * All the possible alert types.
     */
    private alertTypes: string[] = ['alert-info', 'alert-warning', 'alert-danger', 'alert-success'];

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var messagesChanges = changes['messages'];

        if (!messagesChanges || _.isEqual(messagesChanges.currentValue, messagesChanges.previousValue) ||
                !_.isArray(messagesChanges.currentValue) || messagesChanges.currentValue.length === 0) {
            return;
        }

        this.messages = messagesChanges.currentValue;
        this.isVisible = true;

        if (this.autoDismiss) {
            this.autoDismissTimeout = setTimeout(() => {
                this.isVisible = false;
            }, this.defaultTimeoutDelay);
        }
    }

    close(): void {
        this.isVisible = false;
    }

    open(): void {
        this.isVisible = true;
    }

    getAlertType(addtionalClasses: string): string {
        if (this.alertTypes.indexOf(this.type) > -1) {
            return this.type + ' ' + addtionalClasses;
        }

        return 'alert-info' + ' ' + addtionalClasses;
    }

    onMoustEnter(event: MouseEvent): void {
        clearTimeout(this.autoDismissTimeout);
    }

    onMouseLeave(event: MouseEvent): void {
        this.autoDismissTimeout = setTimeout(() => {
            this.isVisible = false;
        }, this.defaultTimeoutDelay);
    }
}
