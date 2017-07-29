// angular
import { AfterViewChecked, Component, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import * as _ from 'lodash';

// app
import { EventContext } from '../../../modules/app/interfaces/index';
import { StringUtil } from '../../../modules/app/utils/index';

declare var Chart: any;

@Component({
    selector: 'xe-dashboard-card',
    moduleId: module.id,
    templateUrl: './dashboard-card.component.html',
    styleUrls: ['./dashboard-card.component.css']
})

export class DashboardCardComponent implements OnChanges, AfterViewChecked, OnDestroy {
    /**
     * The id of the card. It's being used as both the selector of the canvas
     * and the key for accessing the correct property within stats.
     */
    @Input()
    cardId: string;

    /**
     * The title of the card being displayed in the view.
     */
    @Input()
    cardTitle: string;

    /**
     * Data used to render the chart. It should include data and labels for
     * all the possible filtering scenarios by orgnazing them in
     * objects whose keys are the filter value. e.g:
     * {
     *      'Filter 1': { data: ..., labels: ... }
     *      'Filter 2': { data: ..., labels: ... }
     * }
     */
    @Input()
    chartData: {[key: string]: any};

    /**
     * The options for rendering the card.
     * {
     *     type: (string) 'line' | 'bar'
     *     color: (string) hex color value
     *     unit: (string) 'percentage' | 'datasize' | empty
     *     filters: (array) [{ name: 'Last Hour', value: 'PerHour' }, { name: 'Last Day', value: 'PerDay' }]
     * }
     */
    @Input()
    chartOptions: any;

    /**
     * Emits an event when the card is clicked by user.
     */
    @Output()
    clickCard = new EventEmitter<EventContext>();

    /**
     * The filter that is being applied to the view.
     */
    private currentFilter: any;

    /**
     * Canvas for rendering the chart
     */
    private canvas: any;

    /**
     * The chart
     */
    private chart: any;

    /**
     * Flag indicates whether the view has been initialized
     */
    private isViewInitialized: boolean = false;

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var chartDataChanges = changes['chartData'];

        if (!chartDataChanges
            || _.isEqual(chartDataChanges.currentValue, chartDataChanges.previousValue)
            || _.isEmpty(chartDataChanges.currentValue)) {
            return;
        }

        this.chartData = chartDataChanges.currentValue;

        // Set currentFilter
        if (!_.isUndefined(this.chartOptions.filters) && !_.isEmpty(this.chartOptions.filters)) {
            this.currentFilter = this.currentFilter
                || _.find(this.chartOptions.filters, { current: true })
                || this.chartOptions.filters[0];
        }

        this.renderChart();
    }

    ngAfterViewChecked(): void {
        if (this.cardId && !_.isUndefined(this.currentFilter) && !this.isViewInitialized) {
            this.isViewInitialized = true;
            this.canvas = document.getElementById(this.cardId);
            this.renderChart();
        }
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.chart)) {
            this.chart.destroy();
        }
    }

    getCardId(): string {
        return this.cardId;
    }

    getCurrentFilter(): any {
        return !_.isUndefined(this.currentFilter) ? this.currentFilter : {};
    }

    getFilters(): any[] {
        return !_.isUndefined(this.chartOptions) ? this.chartOptions.filters : [];
    }

    getRollUpStat(): string {
        if (_.isUndefined(this.chartData) || _.isEmpty(this.chartData)
            || _.isUndefined(this.chartOptions) || _.isEmpty(this.chartOptions)) {
            return '';
        }

        var data: number[] = this.chartData[`${this.currentFilter.value}`].data;
        var type: string = this.chartData[`${this.currentFilter.value}`].type;
        var options: any = this.chartData[`${this.currentFilter.value}`].options
            || this.chartOptions;

        switch (type || options.type) {
            case 'line':
                return this.formatData(_.last(data), options.unit);
            case 'bar':
                return this.formatData(_.sum(data), options.unit);
            default:
                return '';
        }
    }

    onFilteredClicked(event: MouseEvent, value: string) {
        event.preventDefault();

        // Update currentFilter
        this.currentFilter = _.find(this.chartOptions.filters, { value: value });
        this.renderChart();
    }

    private renderChart(): void {
        if (_.isUndefined(this.canvas) || _.isNull(this.canvas)) {
            return;
        }

        // Destroy and recreate the chart if it has been created before
        if (!_.isUndefined(this.chart)) {
            this.chart.destroy();
        }

        // Render the chart
        this.chart = new Chart(this.canvas, this.getChartOptions());
    }

    /**
     * Returns options for rendering the chart, which also include labels
     * and data.
     */
    private getChartOptions(): any {
        if (_.isUndefined(this.chartData) || _.isEmpty(this.chartData)
            || _.isUndefined(this.chartOptions) || _.isEmpty(this.chartOptions)) {
            return {};
        }

        var data: number[] = this.chartData[`${this.currentFilter.value}`].data;
        var labels: string[] = this.chartData[`${this.currentFilter.value}`].labels;
        var type: string = this.chartData[`${this.currentFilter.value}`].type;
        var options: any = this.chartData[`${this.currentFilter.value}`].options
            || this.chartOptions;

        switch (type || options.type) {
            case 'line':
                return this.getLineChartOptions(data, labels, options);
            case 'bar':
                return this.getBarChartOptions(data, labels, options);
            default:
                return {};
        }
    }

    /**
     * Returns options specificly for rendering line charts.
     */
    private getLineChartOptions(data: number[], labels: string[], options: any): any {
        var rgbColor: string = this.convertHexToRgb(options.color);

        return {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    fill: true,
                    lineTension: 0.1,
                    backgroundColor: `rgba(${rgbColor}, .25)`,
                    borderColor: `rgba(${rgbColor}, 1)`,
                    borderJoinStyle: 'round',
                    borderCapStyle: 'butt',
                    borderWidth: 4,
                    pointRadius: 1,
                    pointHitDetectionRadius : 10,
                    pointBackgroundColor: '#fff',
                    pointBorderWidth: 1,
                    pointHoverRadius: 4,
                    pointHoverBackgroundColor: `#fff`,
                    pointHoverBorderColor: `rgba(${rgbColor}, 1)`,
                    pointHoverBorderWidth: 3
                }]
            },
            options: {
                scales: {
                    xAxes: [{
                        ticks: {
                            maxTicksLimit: 5,
                            maxRotation: 0
                        },
                        gridLines: {
                            display: false
                        }
                    }],
                    yAxes: [{
                        display: false,
                        ticks: {
                            beginAtZero: false,
                            min: _.min(data) * 0.5,
                            max: _.max(data) * 1.5
                        }
                    }]
                },
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                this.formatData(data.datasets[datasetIndex].data[dataIndex], options.unit);

                            return `${formattedValue}`;
                        }
                    }
                }
            }
        };
    }

    private getBarChartOptions(data: number[], labels: string[], options: any): any {
        var backgroundColor: string[] = [];
        var borderColor: string[] = [];
        if (_.isArray(options.color)) {
            _.each(options.color, (color: string) => {
                let rgbColor: string = this.convertHexToRgb(color);

                backgroundColor.push(`rgba(${rgbColor}, .25)`);
                borderColor.push(`rgba(${rgbColor}, 1)`);
            });
        } else {
            let rgbColor: string = this.convertHexToRgb(options.color);

            backgroundColor.push(`rgba(${rgbColor}, .25)`);
            borderColor.push(`rgba(${rgbColor}, 1)`);
        }

        return {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    backgroundColor: backgroundColor,
                    borderColor: borderColor,
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
                        gridLines: {
                            display: false
                        }
                    }]
                },
                tooltips: {
                    callbacks: {
                        label: (tooltip: any, data: any) => {
                            var dataIndex: number = tooltip.index;
                            var datasetIndex: number = tooltip.datasetIndex;
                            var formattedValue: string =
                                this.formatData(data.datasets[datasetIndex].data[dataIndex], options.unit);

                            return `${formattedValue}`;
                        }
                    }
                }
            }
        };
    }

    /**
     * Format the given data based on instructions provided through "chartOptions" input
     */
    private formatData(data: number, unit: string): string {
        switch (unit) {
            case 'percentage':
                return StringUtil.formatPercentage(data);
            case 'datasize':
                return StringUtil.formatDataSize(data);
            case 'number':
            default:
                return StringUtil.formatNumber(data);
        }
    }

    /**
     * Convert a given hex color string to a comma-separated rgb string
     */
    private convertHexToRgb(colorInHex: string): string {
        // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
        var shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
        var hex = colorInHex.replace(shorthandRegex, function(m, r, g, b) {
            return r + r + g + g + b + b;
        });

        var rgb = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return rgb ? `${parseInt(rgb[1], 16)}, ${parseInt(rgb[2], 16)}, ${parseInt(rgb[3], 16)}`: '';
    }
}
