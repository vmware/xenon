// angular
import { AfterViewChecked, Component, Input,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { animate, state, style, transition, trigger } from '@angular/animations';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { ModalContext, ServiceDocument, ServiceDocumentQueryResult,
    ServiceStats, ServiceStatsEntry } from '../../../modules/app/interfaces/index';
import { StringUtil } from '../../../modules/app/utils/index';
import { BaseService, NotificationService } from '../../../modules/app/services/index';

declare var Chart: any;

const OPERATIONS: string[] = ['GET', 'POST', 'PATCH', 'PUT', 'OPTIONS'];

@Component({
    selector: 'xe-child-service-detail',
    moduleId: module.id,
    templateUrl: './child-service-detail.component.html',
    styleUrls: ['./child-service-detail.component.css'],
    animations: [
        trigger('statsState', [
            state('expand', style({
                width: '384px',
                padding: '24px 36px'
            })),
            state('collapse', style({
                width: '0',
                padding: '24px 0'
            })),
            transition('expand <=> collapse', animate('200ms ease-out'))
        ])
    ]
})

export class ChildServiceDetailComponent implements AfterViewChecked, OnChanges, OnDestroy {
    /**
     * Id for the selected service. E.g. /core/examples
     */
    @Input()
    selectedServiceId: string;

    /**
     * Id for the selected child service.
     */
    @Input()
    selectedChildServiceId: string;

    /**
     * Context object for rendering edit child service modal.
     */
    editChildServiceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: '',
            method: '',
            body: ''
        }
    };

    /**
     * Context object for rendering delete child service modal.
     */
    deleteChildServiceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: ''
        }
    };

    statsState: string = 'expand';

    /**
     * Selected child service document
     */
    private selectedChildService: ServiceDocumentQueryResult;

    /**
     * Selected child service stats document
     */
    private selectedChildServiceStats: ServiceStats;

    /**
     * Canvas for rendering operation count chart
     */
    private operationCountChartCanvas: any;

    /**
     * The operation count chart
     */
    private operationCountChart: any;

    /**
     * Canvas for rendering operation duration chart
     */
    private operationDurationChartCanvas: any;

    /**
     * The operation duration chart
     */
    private operationDurationChart: any;

    /**
     * Flag indicates whether the view has been initialized
     */
    private isViewInitialized: boolean = false;

    /**
     * Subscriptions to services.
     */
    private baseServiceGetChildServiceSubscription: Subscription;
    private baseServiceGetChildServiceStatsSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var selectedChildServiceIdChanges = changes['selectedChildServiceId'];

        if (!selectedChildServiceIdChanges
            || _.isEqual(selectedChildServiceIdChanges.currentValue, selectedChildServiceIdChanges.previousValue)
            || _.isEmpty(selectedChildServiceIdChanges.currentValue)) {
            return;
        }

        this.selectedChildServiceId = selectedChildServiceIdChanges.currentValue;

        this.getData();
    }

    ngAfterViewChecked(): void {
        if (this.isViewInitialized) {
            return;
        }

        this.operationCountChartCanvas = document.getElementById('operationCountChart');
        this.operationDurationChartCanvas = document.getElementById('operationDurationChart');

        if (_.isNull(this.operationCountChartCanvas) || _.isNull(this.operationDurationChartCanvas)) {
            return;
        }

        this.isViewInitialized = true;
        this.renderChart();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.operationCountChart)) {
            this.operationCountChart.destroy();
        }

        if (!_.isUndefined(this.operationDurationChart)) {
            this.operationDurationChart.destroy();
        }

        if (!_.isUndefined(this.baseServiceGetChildServiceSubscription)) {
            this.baseServiceGetChildServiceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetChildServiceStatsSubscription)) {
            this.baseServiceGetChildServiceStatsSubscription.unsubscribe();
        }
    }

    getSelectedChildService(): ServiceDocumentQueryResult {
        return this.selectedChildService;
    }

    getTimeline(): any[] {
        if (_.isUndefined(this.selectedChildServiceStats)) {
            return [];
        }

        var operationEntries: {[key: string]: ServiceStatsEntry} = this.selectedChildServiceStats.entries;
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

    onEditChildServiceMethodChanged(event: MouseEvent, method: string) {
        this.editChildServiceModalContext.data['method'] = method;

        if (method === 'PATCH' && !_.isUndefined(this.selectedChildService)) {
            this.editChildServiceModalContext.data['body'] = this.getPatchMethodDefaultBody();
        } else {
            this.editChildServiceModalContext.data['body'] = '';
        }
    }

    onEditChildService(event: MouseEvent): void {
        var selectedServiceId: string = this.editChildServiceModalContext.data['documentSelfLink'];
        var method: string = this.editChildServiceModalContext.data['method'];
        var body: string = this.editChildServiceModalContext.data['body'];

        if (!selectedServiceId || !method || !body) {
            return;
        }

        if (method === 'PATCH') {
            this.baseService.patch(selectedServiceId, body).subscribe(
                (document: ServiceDocument) => {
                    this.notificationService.set([{
                        type: 'SUCCESS',
                        messages: [`Child Service ${document.documentSelfLink} Updated`]
                    }]);

                    // Reset body
                    this.editChildServiceModalContext.data['body'] = null;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`[${error.statusCode}] ${error.message}`]
                    }]);
                });
        } else if (method === 'PUT') {
            this.baseService.put(selectedServiceId, body).subscribe(
                (document: ServiceDocument) => {
                    this.notificationService.set([{
                        type: 'SUCCESS',
                        messages: [`Child Service ${document.documentSelfLink} Updated`]
                    }]);

                    // Reset body
                    this.editChildServiceModalContext.data['body'] = null;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`[${error.statusCode}] ${error.message}`]
                    }]);
                });
        } else {
            this.notificationService.set([{
                type: 'ERROR',
                messages: [`Unknown method ${method} used to modify the child service`]
            }]);
        }
    }

    onDeleteChildService(event: MouseEvent): void {
        var selectedServiceId: string = this.deleteChildServiceModalContext.data['documentSelfLink'];

        if (!selectedServiceId) {
            return;
        }

        this.baseService.delete(selectedServiceId).subscribe(
            (document: ServiceDocument) => {
                this.notificationService.set([{
                    type: 'SUCCESS',
                    messages: [`Child Service ${document.documentSelfLink} Deleted`]
                }]);
            },
            (error) => {
                // TODO: Better error handling
                this.notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    onToggleStats(): void {
        this.statsState = this.statsState === 'expand' ? 'collapse' : 'expand';
    }

    private getData(): void {
        var link: string = this.selectedServiceId + '/' + this.selectedChildServiceId;

        this.baseServiceGetChildServiceSubscription =
            this.baseService.getDocument(link).subscribe(
                (childService: ServiceDocumentQueryResult) => {
                    this.selectedChildService = childService;

                    // Set modal context
                    this.editChildServiceModalContext.name = link;
                    this.editChildServiceModalContext.data['documentSelfLink'] = link;
                    this.editChildServiceModalContext.data['method'] = 'PATCH';
                    this.editChildServiceModalContext.data['body'] = this.getPatchMethodDefaultBody();

                    this.deleteChildServiceModalContext.name = link;
                    this.deleteChildServiceModalContext.data['documentSelfLink'] = link;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory child service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this.baseServiceGetChildServiceStatsSubscription =
            this.baseService.getDocumentStats(link).subscribe(
                (childServiceStats: ServiceStats) => {
                    this.selectedChildServiceStats = childServiceStats;

                    this.renderChart();
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory child service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    private renderChart(): void {
        if (_.isUndefined(this.selectedChildServiceStats)
            || _.isNull(this.operationCountChartCanvas)
            || _.isNull(this.operationDurationChartCanvas)) {
            return;
        }

        if (!_.isUndefined(this.operationCountChart)) {
            this.operationCountChart.destroy();
        }

        if (!_.isUndefined(this.operationDurationChart)) {
            this.operationDurationChart.destroy();
        }

        var operationEntries: {[key: string]: ServiceStatsEntry} = this.selectedChildServiceStats.entries;

        var operationsCounts: number[] = _.map(OPERATIONS, (operation: string) => {
            var property: string = `${operation}requestCount`;
            return operationEntries.hasOwnProperty(property) ? operationEntries[property].latestValue : 0;
        });
        var operationDurations: number[] = _.map(OPERATIONS, (operation: string) => {
            var property: string = `${operation}operationDuration`;
            return operationEntries.hasOwnProperty(property) ? operationEntries[property].latestValue : 0;
        });

        // Render operation count chart
        this.operationCountChart = new Chart(this.operationCountChartCanvas, {
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

        this.operationDurationChart = new Chart(this.operationDurationChartCanvas, {
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

    private getPatchMethodDefaultBody(): string {
        if (_.isUndefined(this.selectedChildService)) {
            return '';
        }

        return JSON.stringify(_.omitBy(this.selectedChildService, (value: any, key: string) => {
            return key.search(/^document[^s]?[A-Z][\w]*/) !== -1;
        }), null, 4);
    }
}
