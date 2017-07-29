// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { Node, ProcessLog, ServiceStats,
    ServiceStatsTimeSeries } from '../../../modules/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../modules/app/services/index';
import { StringUtil } from '../../../modules/app/utils/index';

const FILTER_PER_HOUR: string = 'PerHour';
const FILTER_PER_DAY: string = 'PerDay';
const FILTER_LOG_ALL: string = 'All';

@Component({
    selector: 'xe-dashboard-grid',
    moduleId: module.id,
    templateUrl: './dashboard-grid.component.html',
    styleUrls: ['./dashboard-grid.component.css']
})

export class DashboardGridComponent implements OnInit, OnDestroy {

    /**
     * Data and configuration for each of the card used in the dashboard.
     * For ease of processing the id/name for each of the card is the common
     * part of the key used in /core/management/stats and the filters are the
     * parts that indicate whether the stats are "PerHour" or "PerDay"
     */
    private cardData: any = {
        cpuUsagePercent: {
            name: 'cpuUsagePercent',
            chartOptions: {
                type: 'line',
                color: '#FFCA28',
                unit: 'percentage',
                filters: [
                    { name: 'Last Hour', value: FILTER_PER_HOUR, current: true },
                    { name: 'Last Day', value: FILTER_PER_DAY, current: false }
                ]
            }
        },
        availableMemoryBytes: {
            name: 'availableMemoryBytes',
            chartOptions: {
                type: 'line',
                color: '#29B6F6',
                unit: 'datasize',
                filters: [
                    { name: 'Last Hour', value: FILTER_PER_HOUR, current: true },
                    { name: 'Last Day', value: FILTER_PER_DAY, current: false }
                ]
            }
        },
        availableDiskByte: {
            name: 'availableDiskByte',
            chartOptions: {
                type: 'line',
                color: '#9CCC65',
                unit: 'datasize',
                filters: [
                    { name: 'Last Hour', value: FILTER_PER_HOUR, current: true },
                    { name: 'Last Day', value: FILTER_PER_DAY, current: false }
                ]
            }
        },
        log: {
            name: 'log',
            chartOptions: {
                type: 'bar',
                color: ['#039BE5', '#FFB300', '#E53935'],
                unit: 'number',
                filters: [
                    { name: 'All', value: FILTER_LOG_ALL, current: true },
                ]
            }
        }
    };

    /**
     * The service stats in the view.
     */
    private stats: ServiceStats;

    /**
     * The process logs in the view.
     */
    private log: ProcessLog;

    /**
     * Subscriptions to services.
     */
    private baseServiceGetStatsSubscription: Subscription;
    private baseServiceGetLogSubscription: Subscription;
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
        if (!_.isUndefined(this.baseServiceGetStatsSubscription)) {
            this.baseServiceGetStatsSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetLogSubscription)) {
            this.baseServiceGetLogSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetSelectedSubscription)) {
            this.nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    /**
     * Prepare data for system charts (CPU, Memory, Disk). It provides chart data
     * and labels for all the possible filtering scenarios by orgnazing them in
     * objects whose keys are the filter value. e.g:
     * {
     *      'Filter 1': { data: ..., labels: ... }
     *      'Filter 2': { data: ..., labels: ... }
     * }
     */
    getSystemChartData(id: string): {[key: string]: any} {
        if (_.isUndefined(this.stats)) {
            return {};
        }

        var perHourStats: ServiceStatsTimeSeries =
                this.stats.entries[`${id}${FILTER_PER_HOUR}`].timeSeriesStats;
        var perHourData: number[] = this.getChartData(perHourStats);
        var perHourLabels: string[] = this.getChartLabels(perHourStats, FILTER_PER_HOUR);

        var perDayStats: ServiceStatsTimeSeries =
                this.stats.entries[`${id}${FILTER_PER_DAY}`].timeSeriesStats;
        var perDayData: number[] = this.getChartData(perDayStats);
        var perDayLabels: string[] = this.getChartLabels(perDayStats, FILTER_PER_DAY);

        var systemChartData: {[key: string]: any} = {};
        systemChartData[`${FILTER_PER_HOUR}`] = {
            data: perHourData,
            labels: perHourLabels
        };
        systemChartData[`${FILTER_PER_DAY}`] = {
            data: perDayData,
            labels: perDayLabels
        };

        return systemChartData;
    }

    getLogChartData(): {[key: string]: any} {
        if (_.isUndefined(this.log)) {
            return {};
        }

        var infoLogCount: number = 0;
        var warningLogCount: number = 0;
        var errorLogCount: number = 0;
        _.each(this.log.items, (logItem: string) => {
            var logItemTypeSegments = logItem.match(StringUtil.LOG_ITEM_TYPE_REGEX);

            if (!logItemTypeSegments || _.isEmpty(logItemTypeSegments)) {
                return;
            }

            var logItemTypeCode = logItemTypeSegments[0].match(/[IWS]/)[0].toUpperCase();

            switch(logItemTypeCode) {
                case 'I':
                    infoLogCount++;
                    break;
                case 'W':
                    warningLogCount++;
                    break;
                case 'S':
                    errorLogCount++;
                    break;
                default:
                    // do nothing
            }
        });

        var logChartData: {[key: string]: any} = {};
        logChartData[`${FILTER_LOG_ALL}`] = {
            data: [infoLogCount, warningLogCount, errorLogCount],
            labels: ['Info', 'Warning', 'Error']
        };
        return logChartData;
    }

    getChartOptions(id: string): any {
        return this.cardData[id].chartOptions;
    }

    private getData(): void {
        this.baseServiceGetStatsSubscription =
            this.baseService.getDocumentStats(URL.CoreManagement).subscribe(
                (stats: ServiceStats) => {
                    this.stats = stats;
                },
                (error) => {// TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve logs: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this.baseServiceGetLogSubscription =
            this.baseService.getDocument(URL.Log).subscribe(
                (log: ProcessLog) => {
                    this.log = log;
                },
                (error) => {// TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve logs: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    /**
     * Returns labels for the chart
     */
    private getChartLabels(timeSeriesStats: ServiceStatsTimeSeries, filter: string): string[] {
        if (_.isUndefined(timeSeriesStats)) {
            return [];
        }

        return _.map(_.sortBy(_.keys(timeSeriesStats.bins), (timestamp: string) => {
                return +timestamp;
            }), (timestamp: string) => {
                // Don't show date when displaying per hour stats
                return StringUtil.getTimeStamp((+timestamp) * 1000,
                    filter === FILTER_PER_HOUR);
            });
    }

    /**
     * Returns data for the chart
     */
    private getChartData(timeSeriesStats: ServiceStatsTimeSeries): number[] {
        if (_.isUndefined(timeSeriesStats)) {
            return [];
        }

        var sortedKeys: string[] = _.sortBy(_.keys(timeSeriesStats.bins), (timestamp: string) => {
                return +timestamp;
            });

        return _.map(sortedKeys, (key: string) => {
            return timeSeriesStats.bins[key].avg;
        });
    }

}
