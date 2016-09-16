// angular
import { AfterViewChecked, ChangeDetectionStrategy, Input, SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

declare var Chart: any;

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { ModalContext, ServiceDocument, ServiceDocumentQueryResult,
    ServiceStats, ServiceStatsEntry } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NotificationService } from '../../../frameworks/app/services/index';

const OPERATIONS: string[] = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE', 'OPTIONS'];

@BaseComponent({
    selector: 'xe-service-instance-detail',
    moduleId: module.id,
    templateUrl: './service-instance-detail.component.html',
    styleUrls: ['./service-instance-detail.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ServiceInstanceDetailComponent implements AfterViewChecked, OnChanges, OnDestroy {
    /**
     * Id for the selected service. E.g. /core/examples
     */
    @Input()
    selectedServiceId: string;

    /**
     * Id for the selected service instance.
     */
    @Input()
    selectedServiceInstanceId: string;

    /**
     * Context object for rendering edit instance modal.
     */
    editInstanceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: '',
            method: '',
            body: ''
        }
    };

    /**
     * Context object for rendering delete instance modal.
     */
    deleteInstanceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: ''
        }
    };

    /**
     * Selected service instance document
     */
    private _selectedServiceInstance: ServiceDocumentQueryResult;

    /**
     * Selected service instance stats document
     */
    private _selectedServiceInstanceStats: ServiceStats;

    /**
     * Canvas for rendering operation count chart
     */
    private _operationCountChartCanvas: any;

    /**
     * The operation count chart
     */
    private _operationCountChart: any;

    /**
     * Canvas for rendering operation duration chart
     */
    private _operationDurationChartCanvas: any;

    /**
     * The operation duration chart
     */
    private _operationDurationChart: any;

    /**
     * Flag indicates whether the view has been initialized
     */
    private _isViewInitialized: boolean = false;

    /**
     * Subscriptions to services.
     */
    private _baseServiceGetServiceInstanceSubscription: Subscription;
    private _baseServiceGetServiceInstanceStatsSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var selectedServiceInstanceIdChanges = changes['selectedServiceInstanceId'];

        if (!selectedServiceInstanceIdChanges
            || _.isEqual(selectedServiceInstanceIdChanges.currentValue, selectedServiceInstanceIdChanges.previousValue)
            || _.isEmpty(selectedServiceInstanceIdChanges.currentValue)) {
            return;
        }

        this.selectedServiceInstanceId = selectedServiceInstanceIdChanges.currentValue;

        this._getData();
    }

    ngAfterViewChecked(): void {
        if (this._isViewInitialized) {
            return;
        }

        this._operationCountChartCanvas = document.getElementById('operationCountChart');
        this._operationDurationChartCanvas = document.getElementById('operationDurationChart');

        if (_.isNull(this._operationCountChartCanvas) || _.isNull(this._operationDurationChartCanvas)) {
            return;
        }

        this._isViewInitialized = true;
        this._renderChart();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._operationCountChart)) {
            this._operationCountChart.destroy();
        }

        if (!_.isUndefined(this._operationDurationChart)) {
            this._operationDurationChart.destroy();
        }

        if (!_.isUndefined(this._baseServiceGetServiceInstanceSubscription)) {
            this._baseServiceGetServiceInstanceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceGetServiceInstanceStatsSubscription)) {
            this._baseServiceGetServiceInstanceStatsSubscription.unsubscribe();
        }
    }

    getSelectedServiceInstance(): ServiceDocumentQueryResult {
        return this._selectedServiceInstance;
    }

    getPropertyArray(properties: {[key: string]: any}): any[] {
        var propertyArray: any[] = [];
        _.each(properties, (property: any, key: string) => {
            propertyArray.push({
                name: key,
                value: _.isObject(property) ? JSON.stringify(property, null, 2) : property
            });
        });

        return propertyArray;
    }

    getTimeline(): any[] {
        if (_.isUndefined(this._selectedServiceInstanceStats)) {
            return [];
        }

        var operationEntries: {[key: string]: ServiceStatsEntry} = this._selectedServiceInstanceStats.entries;
        var timeline: any[] = [];

        _.each(OPERATIONS, (operation: string) => {
            var property: string = `${operation}requestCount`;

            if (operationEntries.hasOwnProperty(property)) {
                timeline.push({
                    name: operation,
                    count: operationEntries[property].latestValue,
                    timestamp: operationEntries[property].lastUpdateMicrosUtc
                });
            }
        });

        return _.orderBy(timeline, ['timestamp'], ['desc']);
    }

    getTimeStamp(timeMicros: number): string {
        return StringUtil.getTimeStamp(timeMicros);
    }

    isNullOrUndefined(value: any) {
        return _.isNull(value) || _.isUndefined(value);
    }

    onEditInstanceMethodChanged(event: MouseEvent, method: string) {
        this.editInstanceModalContext.data['method'] = method;

        if (method === 'PATCH' && !_.isUndefined(this._selectedServiceInstance)) {
            this.editInstanceModalContext.data['body'] = this._getPatchMethodDefaultBody();
        } else {
            this.editInstanceModalContext.data['body'] = '';
        }
    }

    onEditInstance(event: MouseEvent): void {
        var selectedServiceId: string = this.editInstanceModalContext.data['documentSelfLink'];
        var method: string = this.editInstanceModalContext.data['method'];
        var body: string = this.editInstanceModalContext.data['body'];

        if (!selectedServiceId || !method || !body) {
            return;
        }

        if (method === 'PATCH') {
            this._baseService.patch(selectedServiceId, body).subscribe(
                (document: ServiceDocument) => {
                    var documentId: string = document.documentSelfLink ?
                        StringUtil.parseDocumentLink(document.documentSelfLink).id : '';
                    this._notificationService.set([{
                        type: 'SUCCESS',
                        messages: [`Instance ${documentId} Patched`]
                    }]);

                    // Reset body
                    this.editInstanceModalContext.data['body'] = null;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`[${error.statusCode}] ${error.message}`]
                    }]);
                });
        } else if (method === 'PUT') {
            this._baseService.put(selectedServiceId, body).subscribe(
                (document: ServiceDocument) => {
                    var documentId: string = document.documentSelfLink ?
                        StringUtil.parseDocumentLink(document.documentSelfLink).id : '';
                    this._notificationService.set([{
                        type: 'SUCCESS',
                        messages: [`Instance ${documentId} Updated`]
                    }]);

                    // Reset body
                    this.editInstanceModalContext.data['body'] = null;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`[${error.statusCode}] ${error.message}`]
                    }]);
                });
        } else {
            this._notificationService.set([{
                type: 'ERROR',
                messages: [`Unknown method ${method} used to modify the instance`]
            }]);
        }
    }

    onDeleteInstance(event: MouseEvent): void {
        var selectedServiceId: string = this.editInstanceModalContext.data['documentSelfLink'];

        if (!selectedServiceId) {
            return;
        }

        this._baseService.delete(selectedServiceId).subscribe(
            (document: ServiceDocument) => {
                var documentId: string = document.documentSelfLink ?
                    StringUtil.parseDocumentLink(document.documentSelfLink).id : '';
                this._notificationService.set([{
                    type: 'SUCCESS',
                    messages: [`Instance ${documentId} Deleted`]
                }]);
            },
            (error) => {
                // TODO: Better error handling
                this._notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    private _getData(): void {
        let link: string = this.selectedServiceId + '/' + this.selectedServiceInstanceId;

        this._baseServiceGetServiceInstanceSubscription =
            this._baseService.getDocument(link).subscribe(
                (serviceInstance: ServiceDocumentQueryResult) => {
                    this._selectedServiceInstance = serviceInstance;

                    // Set modal context
                    this.editInstanceModalContext.name = link;
                    this.editInstanceModalContext.data['documentSelfLink'] = link;
                    this.editInstanceModalContext.data['method'] = 'PATCH';
                    this.editInstanceModalContext.data['body'] = this._getPatchMethodDefaultBody();

                    this.deleteInstanceModalContext.name = link;
                    this.deleteInstanceModalContext.data['documentSelfLink'] = link;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service instance details: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this._baseServiceGetServiceInstanceStatsSubscription =
            this._baseService.getDocumentStats(link).subscribe(
                (serviceInstanceStats: ServiceStats) => {
                    this._selectedServiceInstanceStats = serviceInstanceStats;

                    this._renderChart();
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service instance details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    private _renderChart(): void {
        if (_.isUndefined(this._selectedServiceInstanceStats)
            || _.isNull(this._operationCountChartCanvas)
            || _.isNull(this._operationDurationChartCanvas)) {
            return;
        }

        if (!_.isUndefined(this._operationCountChart)) {
            this._operationCountChart.destroy();
        }

        if (!_.isUndefined(this._operationDurationChart)) {
            this._operationDurationChart.destroy();
        }

        var operationEntries: {[key: string]: ServiceStatsEntry} = this._selectedServiceInstanceStats.entries;

        var operationsCounts: number[] = _.map(OPERATIONS, (operation: string) => {
            var property: string = `${operation}requestCount`;
            return operationEntries.hasOwnProperty(property) ? operationEntries[property].latestValue : 0;
        });
        var operationDurations: number[] = _.map(OPERATIONS, (operation: string) => {
            var property: string = `${operation}operationDuration`;
            return operationEntries.hasOwnProperty(property) ? operationEntries[property].latestValue : 0;
        });

        // Render operation count chart
        this._operationCountChart = new Chart(this._operationCountChartCanvas, {
            type: 'radar',
            data: {
                labels: OPERATIONS,
                datasets: [{
                    data: operationsCounts,
                    backgroundColor: 'rgba(38, 198, 218, .25)',
                    pointRadius: 3,
                    borderColor: 'rgba(38, 198, 218, 1)',
                    pointBackgroundColor: 'rgba(38, 198, 218, 1)',
                    pointBorderColor: '#fff',
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: '#fff',
                    pointHoverBorderColor: 'rgba(38, 198, 218, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                scale: {
                    ticks: {
                        maxTicksLimit: 4
                    }
                },
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                StringUtil.formatNumber(data.datasets[datasetIndex].data[dataIndex]);

                            return `${formattedValue} requests`;
                        }
                    }
                }
            }
        });

        this._operationDurationChart = new Chart(this._operationDurationChartCanvas, {
            type: 'bar',
            data: {
                labels: OPERATIONS,
                datasets: [{
                    data: operationDurations,
                    backgroundColor: 'rgba(255, 202, 40, .25)',
                    borderColor: 'rgba(255, 202, 40, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                scales: {
                    xAxes: [{
                        gridLines: {
                            display: false
                        }
                    }],
                    yAxes: [{
                        display: false
                    }]
                },
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                StringUtil.formatDurationToMilliseconds(data.datasets[datasetIndex].data[dataIndex]);

                            return `${formattedValue} ms`;
                        }
                    }
                }
            }
        });
    }

    private _getPatchMethodDefaultBody(): string {
        if (_.isUndefined(this._selectedServiceInstance)) {
            return '';
        }

        return JSON.stringify(_.omitBy(this._selectedServiceInstance, (value: any, key: string) => {
            return key.search(/^document[^s]?[A-Z][\w]*/) !== -1;
        }), null, 4);
    }
}
