// angular
import { AfterViewChecked, ChangeDetectionStrategy, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { EventContext } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

declare var Chart: any;

@BaseComponent({
    selector: 'xe-dashboard-card',
    moduleId: module.id,
    templateUrl: './dashboard-card.component.html',
    styleUrls: ['./dashboard-card.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
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
    private _currentFilter: any;

    /**
     * Canvas for rendering the chart
     */
    private _canvas: any;

    /**
     * The chart
     */
    private _chart: any;

    /**
     * Flag indicates whether the view has been initialized
     */
    private _isViewInitialized: boolean = false;

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
            this._currentFilter = this._currentFilter
                || _.find(this.chartOptions.filters, { current: true })
                || this.chartOptions.filters[0];
        }

        this._renderChart();
    }

    ngAfterViewChecked(): void {
        if (this.cardId && !_.isUndefined(this._currentFilter) && !this._isViewInitialized) {
            this._isViewInitialized = true;
            this._canvas = document.getElementById(this.cardId);
            this._renderChart();
        }
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._chart)) {
            this._chart.destroy();
        }
    }

    getCardId(): string {
        return this.cardId;
    }

    getCurrentFilter(): any {
        return !_.isUndefined(this._currentFilter) ? this._currentFilter : {};
    }

    getFilters(): any[] {
        return !_.isUndefined(this.chartOptions) ? this.chartOptions.filters : [];
    }

    onFilteredClicked(event: MouseEvent, value: string) {
        event.preventDefault();

        // Update currentFilter
        this._currentFilter = _.find(this.chartOptions.filters, { value: value });
        this._renderChart();
    }

    private _renderChart(): void {
        if (_.isUndefined(this._canvas) || _.isNull(this._canvas)) {
            return;
        }

        // Destroy and recreate the chart if it has been created before
        if (!_.isUndefined(this._chart)) {
            this._chart.destroy();
        }

        // Render the chart
        this._chart = new Chart(this._canvas, this._getChartOptions());
    }

    /**
     * Returns options for rendering the chart, which also include labels
     * and data.
     */
    private _getChartOptions(): any {
        if (_.isUndefined(this.chartData) || _.isEmpty(this.chartData)
            || _.isUndefined(this.chartOptions) || _.isEmpty(this.chartOptions)) {
            return {};
        }

        var data: number[] = this.chartData[`${this._currentFilter.value}`].data;
        var labels: string[] = this.chartData[`${this._currentFilter.value}`].labels;
        var type: string = this.chartData[`${this._currentFilter.value}`].type;
        var options: any = this.chartData[`${this._currentFilter.value}`].options
            || this.chartOptions;

        switch (type || options.type) {
            case 'line':
                return this._getLineChartOptions(data, labels, options);
            case 'bar':
                return this._getBarChartOptions(data, labels, options);
            default:
                return {};
        }
    }

    /**
     * Returns options specificly for rendering line charts.
     */
    private _getLineChartOptions(data: number[], labels: string[], options: any): any {
        var rgbColor: string = this._convertHexToRgb(options.color);

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
                                this._formatData(data.datasets[datasetIndex].data[dataIndex], options.unit);

                            return `${formattedValue}`;
                        }
                    }
                }
            }
        };
    }

    private _getBarChartOptions(data: number[], labels: string[], options: any): any {
        var backgroundColor: string[] = [];
        var borderColor: string[] = [];
        if (_.isArray(options.color)) {
            _.each(options.color, (color: string) => {
                let rgbColor: string = this._convertHexToRgb(color);

                backgroundColor.push(`rgba(${rgbColor}, .25)`);
                borderColor.push(`rgba(${rgbColor}, 1)`);
            });
        } else {
            let rgbColor: string = this._convertHexToRgb(options.color);

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
                                this._formatData(data.datasets[datasetIndex].data[dataIndex], options.unit);

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
    private _formatData(data: number, unit: string): string {
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
    private _convertHexToRgb(colorInHex: string): string {
        // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
        var shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
        var hex = colorInHex.replace(shorthandRegex, function(m, r, g, b) {
            return r + r + g + g + b + b;
        });

        var rgb = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return rgb ? `${parseInt(rgb[1], 16)}, ${parseInt(rgb[2], 16)}, ${parseInt(rgb[3], 16)}`: '';
    }
}
