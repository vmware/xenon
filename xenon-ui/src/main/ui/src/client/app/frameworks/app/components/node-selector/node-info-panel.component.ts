// angular
import { AfterViewInit, ChangeDetectionStrategy, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';

// app
import { BaseComponent } from '../../../core/index';

import { URL } from '../../enums/index';
import { ServiceHostState, SystemHostInfo } from '../../interfaces/index';
import { OsUtil, StringUtil } from '../../utils/index';
import { BaseService, NotificationService } from '../../services/index';

declare var Chart: any;

@BaseComponent({
    selector: 'xe-node-info-panel',
    moduleId: module.id,
    templateUrl: './node-info-panel.component.html',
    styleUrls: ['./node-info-panel.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class NodeInfoPanelComponent implements OnChanges, AfterViewInit, OnDestroy {
    /**
     * The id of the node that hosts the current application.
     */
    @Input()
    hostNodeId: string;

    /**
     * The id of the node whose information is being displayed in the views (not the node selector).
     */
    @Input()
    selectedNodeId: string;

    /**
     * The id of the node that is temporary being highlighted in the node selector.
     * Until user confirms, the node will not become selected node.
     */
    @Input()
    highlightedNodeId: string;

    /**
     * Emit the event with the node id when user confirms to make a given node the selected node.
     */
    @Output()
    selectNode = new EventEmitter<string>();

    /**
     * Details of the highlighted node
     */
    private _highlightedNodeDetails: ServiceHostState;

    /**
     * Canvas for rendering memory chart
     */
    private _memoryChartCanvas: any;

    /**
     * The memory chart
     */
    private _memoryChart: any;

    /**
     * Canvas for rendering memory chart
     */
    private _diskChartCanvas: any;

    /**
     * The disk chart
     */
    private _diskChart: any;

    /**
     * Subscriptions to services.
     */
    private _baseServiceSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var highlightedNodeIdChanges = changes['highlightedNodeId'];

        if (!highlightedNodeIdChanges
            || _.isEqual(highlightedNodeIdChanges.currentValue, highlightedNodeIdChanges.previousValue)
            || _.isEmpty(highlightedNodeIdChanges.currentValue)) {
            return;
        }

        this.highlightedNodeId = highlightedNodeIdChanges.currentValue;

        // Retrieve the selected node details whenever the selection changes
        this._retrieveHighlightedNodeDetails();
    }

    ngAfterViewInit(): void {
        this._memoryChartCanvas = document.getElementById('memoryChart');
        this._diskChartCanvas = document.getElementById('diskChart');

        // Global chart setting is done here since the component itself is a
        // global component.
        Chart.defaults.global.title.display = false;
        Chart.defaults.global.legend.display = false;
        Chart.defaults.global.animation.duration = 200;

        // Tooltip. Consistant with other stylings defined in CSS
        Chart.defaults.global.tooltips.bodyFontFamily = '"Helvetica Neue", "Arial", sans-serif';
        Chart.defaults.global.tooltips.bodyFontSize = 16;
        Chart.defaults.global.tooltips.backgroundColor = 'rgba(33, 33, 33, .75)';
        Chart.defaults.global.tooltips.cornerRadius = 4;
        Chart.defaults.global.tooltips.xPadding = 12;
        Chart.defaults.global.tooltips.yPadding = 12;
        Chart.defaults.global.tooltips.titleSpacing = 0;
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceSubscription)) {
            this._baseServiceSubscription.unsubscribe();
        }

        // Destroy charts
        if (!_.isUndefined(this._memoryChart)) {
            this._memoryChart.destroy();
        }

        if (!_.isUndefined(this._diskChart)) {
            this._diskChart.destroy();
        }
    }

    getHighlightedNodeDetails(): ServiceHostState {
        return this._highlightedNodeDetails;
    }

    getStateSuffix(): string {
        var states: string[] = [];

        if (this.hostNodeId && this.hostNodeId === this.highlightedNodeId) {
            states.push('Host');
        }

        if (this.selectedNodeId && this.selectedNodeId === this.highlightedNodeId) {
            states.push('Current');
        }

        return states.length !==  0 ? ` (${states.join(', ')})` : '';
    }

    getMemoryUsagePercentage(): string {
        if (_.isUndefined(this._highlightedNodeDetails)
            || _.isUndefined(this._highlightedNodeDetails.systemInfo)) {
            return '0';
        }

        var systemInfo: SystemHostInfo = this._highlightedNodeDetails.systemInfo;

        return ((1 - systemInfo.freeMemoryByteCount / systemInfo.totalMemoryByteCount) * 100).toPrecision(3);
    }

    getDiskUsagePercentage(): string {
        if (_.isUndefined(this._highlightedNodeDetails)
            || _.isUndefined(this._highlightedNodeDetails.systemInfo)) {
            return '0';
        }

        var systemInfo: SystemHostInfo = this._highlightedNodeDetails.systemInfo;

        return ((1 - systemInfo.freeDiskByteCount / systemInfo.totalDiskByteCount) * 100).toPrecision(3);
    }

    getOsIconClass(osFamily: string): string {
        return osFamily ? OsUtil[osFamily.toUpperCase()].iconClassName : '';
    }

    formatTimeStamp(milliseconds: number): string {
        return StringUtil.getTimeStamp(milliseconds);
    }

    formatDurationInSeconds(milliseconds: number): string {
        return StringUtil.formatDurationToSeconds(milliseconds) + ' Seconds';
    }

    formatDataSize(bytes: number): string {
        return bytes !== 0 ? StringUtil.formatDataSize(bytes) : 'N/A';
    }

    formatNumber(n: number): string {
        return StringUtil.formatNumber(n);
    }

    isHostNode(): boolean {
        return this.highlightedNodeId === this.hostNodeId;
    }

    isSelectedNode(): boolean {
        return this.highlightedNodeId === this.selectedNodeId;
    }

    onSelectNodeBtnClicked(evnet: Event): void {
        this.selectNode.emit(this.highlightedNodeId);
    }

    private _retrieveHighlightedNodeDetails(): void {
        var url: string;

        if (this.isHostNode()) {
            url = URL.CoreManagement;
        } else {
            url = `${URL.FORWARDING_PATH}?peer=${this.highlightedNodeId}&path=${URL.CoreManagement}&target=PEER_ID`;
        }

        this._baseServiceSubscription = this._baseService.getDocument(url, false).subscribe(
            (serviceHostState: ServiceHostState) => {
                this._highlightedNodeDetails = serviceHostState;

                // Render charts
                this._renderChart();
            },
            (error) => {
                // TODO: Better error handling
                this._notificationService.set([{
                    type: 'ERROR',
                    messages: [`Failed to retrieve node details: [${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    private _renderChart(): void {
        if (_.isUndefined(this._highlightedNodeDetails)
            || _.isUndefined(this._highlightedNodeDetails.systemInfo)) {
            return;
        }

        if (!_.isUndefined(this._memoryChart)) {
            this._memoryChart.destroy();
        }

        if (!_.isUndefined(this._diskChart)) {
            this._diskChart.destroy();
        }

        var systemInfo: SystemHostInfo = this._highlightedNodeDetails.systemInfo;

        // Render memory chart
        this._memoryChart = new Chart(this._memoryChartCanvas, {
            type: 'doughnut',
            data: {
                labels: ['Used Memory', 'Available Memory'],
                datasets: [{
                    data: [systemInfo.totalMemoryByteCount - systemInfo.freeMemoryByteCount,
                        systemInfo.freeMemoryByteCount],
                    backgroundColor: [
                        'rgba(41, 182, 246, .25)',
                        'rgba(189, 189, 189, .25)'
                    ],
                    borderColor: [
                        'rgba(41, 182, 246, 1)',
                        'rgba(189, 189, 189, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                cutoutPercentage: 80,
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                StringUtil.formatDataSize(data.datasets[datasetIndex].data[dataIndex]);

                            return `${data.labels[dataIndex]}: ${formattedValue}`;
                        }
                    }
                }
            }
        });

        this._diskChart = new Chart(this._diskChartCanvas, {
            type: 'doughnut',
            data: {
                labels: ['Used Disk', 'Available Disk'],
                datasets: [{
                    data: [systemInfo.totalDiskByteCount - systemInfo.freeDiskByteCount, systemInfo.freeDiskByteCount],
                    backgroundColor: [
                        'rgba(156, 204, 101, .25)',
                        'rgba(189, 189, 189, .25)'
                    ],
                    borderColor: [
                        'rgba(156, 204, 101, 1)',
                        'rgba(189, 189, 189, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                cutoutPercentage: 80,
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                StringUtil.formatDataSize(data.datasets[datasetIndex].data[dataIndex]);

                            return `${data.labels[dataIndex]}: ${formattedValue}`;
                        }
                    }
                }
            }
        });
    }
}
