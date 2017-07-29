// angular
import { AfterViewInit, Component, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../enums/index';
import { ServiceHostState, SystemHostInfo, UrlFragment } from '../../interfaces/index';
import { OsUtil, StringUtil } from '../../utils/index';
import { BaseService, NotificationService } from '../../services/index';

declare var Chart: any;

@Component({
    selector: 'xe-node-info-panel',
    moduleId: module.id,
    templateUrl: './node-info-panel.component.html',
    styleUrls: ['./node-info-panel.component.css']
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
     * The reference to the node group whose node inside it is highlighted.
     */
    @Input()
    highlightedNodeGroupReference: string;

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
     * Whether or not the highlighted node can be switched to
     */
    canSwitchToNode: boolean = false;

    /**
     * Details of the highlighted node
     */
    private highlightedNodeDetails: ServiceHostState;

    /**
     * Canvas for rendering memory chart
     */
    private memoryChartCanvas: any;

    /**
     * The memory chart
     */
    private memoryChart: any;

    /**
     * Canvas for rendering memory chart
     */
    private diskChartCanvas: any;

    /**
     * The disk chart
     */
    private diskChart: any;

    /**
     * Subscriptions to services.
     */
    private baseServiceSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var highlightedNodeIdChanges = changes['highlightedNodeId'];

        if (!highlightedNodeIdChanges
            || _.isEqual(highlightedNodeIdChanges.currentValue, highlightedNodeIdChanges.previousValue)
            || _.isEmpty(highlightedNodeIdChanges.currentValue)) {
            return;
        }

        this.highlightedNodeId = highlightedNodeIdChanges.currentValue;

        // Retrieve the selected node details whenever the selection changes
        this.retrieveHighlightedNodeDetails();
    }

    ngAfterViewInit(): void {
        this.memoryChartCanvas = document.getElementById('memoryChart');
        this.diskChartCanvas = document.getElementById('diskChart');

        // Global chart setting is done here since the component itself is a
        // global component.
        Chart.defaults.global.title.display = false;
        Chart.defaults.global.legend.display = false;
        Chart.defaults.global.animation.duration = 200;

        // Tooltip. Consistant with other stylings defined in CSS
        Chart.defaults.global.tooltips.displayColors = false;
        Chart.defaults.global.tooltips.bodyFontFamily = '"Helvetica Neue", "Arial", sans-serif';
        Chart.defaults.global.tooltips.bodyFontSize = 16;
        Chart.defaults.global.tooltips.backgroundColor = 'rgba(33, 33, 33, .75)';
        Chart.defaults.global.tooltips.cornerRadius = 4;
        Chart.defaults.global.tooltips.xPadding = 12;
        Chart.defaults.global.tooltips.yPadding = 12;
        Chart.defaults.global.tooltips.titleSpacing = 0;
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceSubscription)) {
            this.baseServiceSubscription.unsubscribe();
        }

        // Destroy charts
        if (!_.isUndefined(this.memoryChart)) {
            this.memoryChart.destroy();
        }

        if (!_.isUndefined(this.diskChart)) {
            this.diskChart.destroy();
        }
    }

    getHighlightedNodeDetails(): ServiceHostState {
        return this.highlightedNodeDetails;
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
        if (_.isUndefined(this.highlightedNodeDetails)
            || _.isUndefined(this.highlightedNodeDetails.systemInfo)) {
            return '0';
        }

        var systemInfo: SystemHostInfo = this.highlightedNodeDetails.systemInfo;

        return ((1 - systemInfo.freeMemoryByteCount / systemInfo.totalMemoryByteCount) * 100).toPrecision(3);
    }

    getDiskUsagePercentage(): string {
        if (_.isUndefined(this.highlightedNodeDetails)
            || _.isUndefined(this.highlightedNodeDetails.systemInfo)) {
            return '0';
        }

        var systemInfo: SystemHostInfo = this.highlightedNodeDetails.systemInfo;

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

    private getForwardingLink(): string {
        let nodeGroupReference: string = this.highlightedNodeGroupReference ?
            StringUtil.parseDocumentLink(this.highlightedNodeGroupReference).id :
            'default';
        return `${URL.NODE_SELECTOR}/${nodeGroupReference}/forwarding?peer=${this.highlightedNodeId}&path=${URL.CoreManagement}&target=PEER_ID`;
    }

    private retrieveHighlightedNodeDetails(): void {
        var url: string;

        if (this.isHostNode()) {
            url = URL.CoreManagement;
        } else {
            url = this.getForwardingLink();
        }

        this.baseServiceSubscription = this.baseService.getDocument(url, '', false).subscribe(
            (serviceHostState: ServiceHostState) => {
                this.highlightedNodeDetails = serviceHostState;

                this.canSwitchToNode = true;

                // Render charts
                this.renderChart();
            },
            (error) => {
                this.canSwitchToNode = false;

                // Create URL for the non-default-group node based on the given
                // highlightedNodeGroupReference.
                var urlFragment: UrlFragment =
                    StringUtil.parseUrl(this.highlightedNodeGroupReference);
                var url: string = `${urlFragment.protocol}//${urlFragment.host}/core/ui/default/#/`;

                // TODO: Better error handling
                this.notificationService.set([{
                    type: 'ERROR',
                    messages: [`Failed to retrieve node details: [${error.statusCode}] ${error.message}.<br>
                        If this node is not in the default node group, try accessing its own
                        <a href="${url}" target="_blank">Xenon UI</a>`]
                }]);
            });
    }

    private renderChart(): void {
        if (_.isUndefined(this.highlightedNodeDetails)
            || _.isUndefined(this.highlightedNodeDetails.systemInfo)) {
            return;
        }

        if (!_.isUndefined(this.memoryChart)) {
            this.memoryChart.destroy();
        }

        if (!_.isUndefined(this.diskChart)) {
            this.diskChart.destroy();
        }

        var systemInfo: SystemHostInfo = this.highlightedNodeDetails.systemInfo;

        // Render memory chart
        this.memoryChart = new Chart(this.memoryChartCanvas, {
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

        this.diskChart = new Chart(this.diskChartCanvas, {
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
