// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { URL } from '../../../frameworks/app/enums/index';
import { Node, ProcessLog } from '../../../frameworks/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

import { ProcessLogTypeUtil } from './process-log-type.util';

@BaseComponent({
    selector: 'xe-process-log',
    moduleId: module.id,
    templateUrl: './process-log.component.html',
    styleUrls: ['./process-log.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ProcessLogComponent implements OnInit, OnDestroy {
    /**
     * The filter that is currently active. It accepts only 4 values:
     * 'all', 'info', 'warning' and 'severe'
     */
    activeFilter: string = 'all';

    /**
     * The log in the view.
     */
    private _log: ProcessLog;

    /**
     * Cached the log items that are info.
     */
    private _infoLogItems: string[] = [];

    /**
     * Cached the log items that are warning.
     */
    private _warningLogItems: string[] = [];

    /**
     * Cached the log items that are severe.
     */
    private _severeLogItems: string[] = [];

    /**
     * RegEx used to locate the type of the log item. Also select its surrounding
     * areas to prevent potential mis-select.
     */
    private _logItemTypeRegEx: any = /\[\d*\]\[[IWS]\]\[\d*\]/i;

    /**
     * Subscriptions to services.
     */
    private _baseServiceSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService) {}

    ngOnInit(): void {
        this._getData();

        // Update data when selected node changes
        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceSubscription)) {
            this._baseServiceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    getLogItems(filter: string): string[] {
        if (_.isEmpty(this._log)) {
            return [];
        }

        var logItems: string[] = [];

        switch(filter) {
            case 'all':
                logItems = this._log.items;
                break;
            case 'info':
                logItems = this._infoLogItems;
                break;
            case 'warning':
                logItems = this._warningLogItems;
                break;
            case 'severe':
                logItems = this._severeLogItems;
                break;
            default:
                // do nothing
        }

        return logItems;
    }

    getLogItemType(logItem: string): string {
        var logItemTypeSegments = logItem.match(this._logItemTypeRegEx);

        if (!logItemTypeSegments || _.isEmpty(logItemTypeSegments)) {
            return '';
        }

        var logItemTypeCode = logItemTypeSegments[0].match(/[IWS]/)[0].toUpperCase();
        var logItemType = '';

        switch(logItemTypeCode) {
            case 'I':
                logItemType = 'INFO';
                break;
            case 'W':
                logItemType = 'WARNING';
                break;
            case 'S':
                logItemType = 'SEVERE';
                break;
            default:
                // do nothing
        }

        return logItemType;
    }

    getLogItemTypeClass(logItem: string, additionalClasses: string): string {
        var logItemType = this.getLogItemType(logItem);

        return ProcessLogTypeUtil.getLogType(logItemType).className + ' ' + additionalClasses;
    }

    getLogItemTypeIconClass(logItem: string): string {
        var logItemType = this.getLogItemType(logItem);

        return ProcessLogTypeUtil.getLogType(logItemType).iconClassName;
    }

    getLogTypeClass(logType: string, additionalClasses: string): string {
        return ProcessLogTypeUtil.getLogType(logType).className + ' ' + additionalClasses;
    }

    getLogTypeIconClass(logType: string): string {
        return ProcessLogTypeUtil.getLogType(logType).iconClassName;
    }

    onFilterApplied(event: MouseEvent, filter: string): void {
        event.stopPropagation();

        this.activeFilter = filter;
    }

    private _getData(): void {
        // Reset the arrays
        this._infoLogItems = [];
        this._warningLogItems = [];
        this._severeLogItems = [];

        this._baseServiceSubscription =
            this._baseService.getDocument(URL.Log).subscribe(
                (log: ProcessLog) => {
                    this._log = log;

                    // Cache log items by type
                    _.each(this._log.items, (logItem) => {
                        var logItemType = this.getLogItemType(logItem);

                        switch(logItemType) {
                            case 'INFO':
                                this._infoLogItems.push(logItem);
                                break;
                            case 'WARNING':
                                this._warningLogItems.push(logItem);
                                break;
                            case 'SEVERE':
                                // TODO: Right now only push the line items
                                // to the cache but not the stack traces since they
                                // are printed as seprate line items. Need to figure out
                                // how to add them to the cache as part of the severe
                                // message.
                                this._severeLogItems.push(logItem);
                                break;
                            default:
                                // do nothing
                        }
                    });
                },
                (error) => {// TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve logs: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
