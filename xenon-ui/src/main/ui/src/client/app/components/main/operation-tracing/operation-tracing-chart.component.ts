// angular
import { AfterViewInit, Component, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import * as _ from 'lodash';
import * as moment from 'moment';

// app
import { EventContext, ModalContext, QueryTask, SerializedOperation,
    Timeline, TimelineEvent } from '../../../modules/app/interfaces/index';
import { StringUtil } from '../../../modules/app/utils/index';

declare var jQuery: any;
declare var d3: any;

@Component({
    selector: 'xe-operation-tracing-chart',
    moduleId: module.id,
    templateUrl: './operation-tracing-chart.component.html',
    styleUrls: ['./operation-tracing-chart.component.css']
})

export class OperationTracingChartComponent implements OnChanges, AfterViewInit, OnDestroy {
    /**
     * Object contains both the original query and the result
     */
    @Input()
    traceResult: QueryTask;

    /**
     * Context object for rendering operation detail modal.
     */
    operationDetailModalContext: ModalContext = {
        name: '',
        data: {
            operations: [], // for group of operations
            operation: {}, // for single operation
            selectedOperation: null, // for group of operations
            operationsTimeRange: null // for group of operations
        }
    };

    /**
     * The canvas to be rendered on
     */
    private canvas: any;

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var traceResultChanges = changes['traceResult'];

        if (!traceResultChanges
            || _.isEmpty(traceResultChanges.currentValue)) {
            return;
        }

        this.render();
    }

    /**
     * Initiate the basic DOM related variables after DOM is fully rendered.
     */
    ngAfterViewInit(): void {
        this.canvas = d3.select('#timeline').append('div');

        this.render();
    }

    ngOnDestroy(): void {
        // Clear all the event handlers
        this.canvas.selectAll('.timeline-pf-drop text')
            .on('click', null)
            .on('mouseover', null);

        this.canvas.selectAll('rect')
            .on('mouseover', null)
            .on('mouseout', null)
            .on('mousemove', null);
    }

    getGroupDetailModalTimelineLeftPosition(operation: TimelineEvent): string {
        var groupOperationsTimeRange: any = this.operationDetailModalContext.data['operationsTimeRange'];

        if (_.isNull(groupOperationsTimeRange)) {
            return '0';
        }

        return (operation.date.valueOf()  - groupOperationsTimeRange.start)
            / (groupOperationsTimeRange.end - groupOperationsTimeRange.start) * 100 - 1.5 // offset 1.5% for the dot size
            + '%';
    }

    private render(): void {
        if ( _.isUndefined(this.canvas)
            || _.isUndefined(this.traceResult) || _.isEmpty(this.traceResult)
            || _.isEmpty(this.traceResult.results)
            || this.traceResult.results.documentCount === 0) {
            return;
        }

        var data: Timeline[] = this.transformData();
        var dataTimeRange: any = this.getDataTimeRange();
        console.log(d3, d3.chart);
        var timeline = d3.chart.timeline({
                labelWidth: 180,
                tickFormat: [
                    ['.%L', (d) => d.getMilliseconds()],
                    [':%S', (d) => d.getSeconds()],
                    ['%I:%M', (d) => d.getMinutes()],
                    ['%I %p', (d) => d.getHours()],
                    ['%m/%d', () => true]
                ],
                eventLineColor: (d: any, i: number) => {
                    var colors: string[] = ['#42a5f5', '#29b6f6', '#26c6da',
                        '#26a69a', '#66bb6a', '#9ccc65', '#d4e157', '#ffee58',
                        '#ffca28', '#ffa726', '#ff7043', '#8d6e63', '#ef5350',
                        '#ec407a', '#ab47bc', '#7e57c2', '#5c6bc0'];
                    return colors[i % colors.length];
                },
                eventPopover: (d: any) => {
                    var popover: string = '';

                    if (d.hasOwnProperty('events')) {
                        popover = `Group of ${d.events.length} operations`;
                    } else {
                        // Populate the detail form for the popover
                        popover +=
                            `<form class="form-timeline-popover"><div class="form-group row">
                                <label class="col-sm-4 col-form-label">ID</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${d.details.object.id}</p>
                                </div>
                            </div>
                            <div class="form-group row">
                                <label class="col-sm-4 col-form-label">Method</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${d.details.event}</p>
                                </div>
                            </div>
                            <div class="form-group row">
                                <label class="col-sm-4 col-form-label">Status</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${d.details.object.statusCode}</p>
                                </div>
                            </div>
                            <div class="form-group row">
                                <label class="col-sm-4 col-form-label">Referer</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${d.details.object.referer}</p>
                                </div>
                            </div>
                            <div class="form-group row">
                                <label class="col-sm-4 col-form-label">Options</label>
                                <div class="col-sm-8">
                                    <div class="tags-container">
                                        ${_.join(_.map(d.details.object.options, (option: string) => {
                                            return '<div class="tag">' + option + '</div>';
                                        }), '')}
                                    </div>
                                </div>
                            </div>
                            <div class="form-group row">
                                <label class="col-sm-4 col-form-label">Host</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${d.details.object.host}:${d.details.object.port}</p>
                                </div>
                            </div>`;
                        popover +=
                            `<div class="form-group row">
                                <label class="col-sm-4 col-form-label">Date</label>
                                <div class="col-sm-8">
                                    <p class="form-control-static">${moment(d.date).format('M/D/YY hh:mm:ss.SSS A')}</p>
                                </div>
                            </div></form>`;
                    }

                    return popover;
                }
            })
            .end(moment(dataTimeRange.end / 1000).add(30, 'seconds').toDate())
            .start(moment(dataTimeRange.start / 1000).subtract(30, 'seconds').toDate())
            .minScale(1 / 7) // till a week
            .maxScale(1 * 60) // till a second
            .eventClick((d: any) => {
                if (d.hasOwnProperty('events')) {
                    // Group events
                    this.operationDetailModalContext.name = `Group of ${d.events.length} operations`;

                    this.operationDetailModalContext.data['operations'] = d.events;
                    this.operationDetailModalContext.data['operation'] = {};

                    let groupOperationsTimeRange: any = this.getGroupOperationsTimeRange(d.events);
                    this.operationDetailModalContext.data['operationsTimeRange'] = groupOperationsTimeRange;
                    this.operationDetailModalContext.data['selectedOperation'] = groupOperationsTimeRange.startOperation;
                } else {
                    // Single event
                    this.operationDetailModalContext.name = `Operation ID: ${d.details.object.id}`;

                    this.operationDetailModalContext.data['operations'] = [];
                    this.operationDetailModalContext.data['operation'] = d;
                    this.operationDetailModalContext.data['operationsTimeRange'] = null;
                    this.operationDetailModalContext.data['selectedOperation'] = null;
                }

                jQuery('#operationDetailModal').modal('show');
            });

        if (this.countNames(data) <= 0) {
            timeline.labelWidth(120);
        }

        this.canvas.datum(data).call(timeline);

        jQuery('[data-toggle="popover"]').popover({
            container: '#timeline',
            placement: 'top',
            trigger: 'hover'
        });
    }

    private getDataTimeRange(): any {
        var dataTimeRange: any = {
            start: 0,
            end: 0
        };

        _.each(this.traceResult.results.documentLinks, (documentLink: string) => {
            var timestamp: number = +documentLink;

            if (dataTimeRange.start === 0) {
                dataTimeRange.start = timestamp;
            }

            if (dataTimeRange.end === 0) {
                dataTimeRange.end = timestamp;
            }

            if (timestamp > dataTimeRange.end) {
                dataTimeRange.end = timestamp;
            } else if (timestamp < dataTimeRange.start) {
                dataTimeRange.start = timestamp;
            }
        });

        return dataTimeRange;
    }

    /**
     * Transform trace result to a format that can be consumed by the chart
     */
    private transformData(): Timeline[] {
        var transformedData: Timeline[] = [];

        _.each(this.traceResult.results.documents, (document: SerializedOperation, key: string) => {
            var eventDetails: TimelineEvent = {
                date: moment(+key / 1000).toDate(),
                details: {
                    event: document.action,
                    object: document
                }
            };

            var matchingData: any = _.find(transformedData, { name: document.path });

            if (!_.isUndefined(matchingData)) {
                matchingData.data.push(eventDetails);
            } else {
                transformedData.push({
                    name: document.path,
                    data: [eventDetails]
                });
            }
        });

        return transformedData;
    }

    private countNames(data: Timeline[]): number {
        var count: number = 0;
        for (let i: number = 0; i < data.length; i++) {
            if (!_.isUndefined(data[i].name) && data[i].name !== '') {
                count++;
            }
        }
        return count;
    }

    private getGroupOperationsTimeRange(operations: TimelineEvent[]): any {
        var groupOperationsTimeRange: any = {
            start: 0,
            startOperation: null,
            end: 0,
            endOperation: null
        };

        _.each(operations, (operation: TimelineEvent) => {
            var timestamp: number = operation.date.valueOf();

            if (groupOperationsTimeRange.start === 0) {
                groupOperationsTimeRange.start = timestamp;
                groupOperationsTimeRange.startOperation = operation;
            }

            if (groupOperationsTimeRange.end === 0) {
                groupOperationsTimeRange.end = timestamp;
                groupOperationsTimeRange.endOperation = operation;
            }

            if (timestamp > groupOperationsTimeRange.end) {
                groupOperationsTimeRange.end = timestamp;
                groupOperationsTimeRange.endOperation = operation;
            } else if (timestamp < groupOperationsTimeRange.start) {
                groupOperationsTimeRange.start = timestamp;
                groupOperationsTimeRange.startOperation = operation;
            }
        });

        return groupOperationsTimeRange;
    }
}
