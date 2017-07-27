// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { Node, ProcessLog } from '../../../modules/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../modules/app/services/index';
import { ODataUtil, StringUtil } from '../../../modules/app/utils/index';

import { ProcessLogTypeUtil } from './process-log-type.util';

@Component({
    selector: 'xe-process-log',
    moduleId: module.id,
    templateUrl: './process-log.component.html',
    styleUrls: ['./process-log.component.css']
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
    private log: ProcessLog;

    /**
     * Cached the log items that are info.
     */
    private infoLogItems: string[] = [];

    /**
     * Cached the log items that are warning.
     */
    private warningLogItems: string[] = [];

    /**
     * Cached the log items that are severe.
     */
    private severeLogItems: string[] = [];

    /**
     * Subscriptions to services.
     */
    private baseServiceSubscription: Subscription;
    private nodeSelectorServiceGetSelectedSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private nodeSelectorService: NodeSelectorService,
        private notificationService: NotificationService) {}

    ngOnInit(): void {
        this.getData();

        // Update data when selected node changes
        this.nodeSelectorServiceGetSelectedSubscription =
            this.nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    this.getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceSubscription)) {
            this.baseServiceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetSelectedSubscription)) {
            this.nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    getLogItems(filter: string): string[] {
        if (_.isEmpty(this.log)) {
            return [];
        }

        var logItems: string[] = [];

        switch(filter) {
            case 'all':
                logItems = this.log.items;
                break;
            case 'info':
                logItems = this.infoLogItems;
                break;
            case 'warning':
                logItems = this.warningLogItems;
                break;
            case 'severe':
                logItems = this.severeLogItems;
                break;
            default:
                // do nothing
        }

        return logItems;
    }

    getFullLogLink(): string {
        return this.baseService.getForwardingLink(URL.Log);
    }

    getLogItemType(logItem: string): string {
        var logItemTypeSegments = logItem.match(StringUtil.LOG_ITEM_TYPE_REGEX);

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

    isPartialLog(): boolean {
        return this.log ? this.log.items.length === ODataUtil.DEFAULT_LOG_SIZE : false;
    }

    onFilterApplied(event: MouseEvent, filter: string): void {
        event.stopPropagation();

        this.activeFilter = filter;
    }

    private getData(): void {
        // Reset the arrays
        this.infoLogItems = [];
        this.warningLogItems = [];
        this.severeLogItems = [];

        this.baseServiceSubscription =
            // Get the logs that limit to the most recent 1000 lines
            this.baseService.getDocument(URL.Log, `lineCount=${ODataUtil.DEFAULT_LOG_SIZE}`).subscribe(
                (log: ProcessLog) => {
                    this.log = log;

                    // Cache log items by type
                    _.each(this.log.items, (logItem) => {
                        var logItemType = this.getLogItemType(logItem);

                        switch(logItemType) {
                            case 'INFO':
                                this.infoLogItems.push(logItem);
                                break;
                            case 'WARNING':
                                this.warningLogItems.push(logItem);
                                break;
                            case 'SEVERE':
                                // TODO: Right now only push the line items
                                // to the cache but not the stack traces since they
                                // are printed as seprate line items. Need to figure out
                                // how to add them to the cache as part of the severe
                                // message.
                                this.severeLogItems.push(logItem);
                                break;
                            default:
                                // do nothing
                        }
                    });
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve logs: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
