// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy, QueryList, ViewChildren } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';
import { URL } from '../../../frameworks/app/enums/index';
import { BooleanClause, EventContext, QuerySpecification, QueryTask,
    ServiceDocument } from '../../../frameworks/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

import { OperationTracingClauseComponent } from './operation-tracing-clause.component';

@BaseComponent({
    selector: 'xe-operation-tracing',
    moduleId: module.id,
    templateUrl: './operation-tracing.component.html',
    styleUrls: ['./operation-tracing.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class OperationTracingComponent implements OnInit, OnDestroy {
    @ViewChildren(OperationTracingClauseComponent)
    operationTracingClauseComponents: QueryList<OperationTracingClauseComponent>;

    traceMethods: string[] = ['GET', 'POST', 'PATCH', 'PUT', 'OPTIONS', 'DELETE'];

    isRecordingOn: boolean = false;

    /**
     * Track the clauses created or removed in interactive mode.
     * NOTE: It does not represent the actual data in the clauses
     */
    traceClauses: any[] = [];

    /**
     * Track the id of the next clause to be added. It starts from 1
     * and will always increase even when some clauses are deleted along
     * the way.
     */
    private _clauseIdTracker: number = 0;

    /**
     * Track the query options in interactive mode
     */
    private _traceOptions: any = {
        traceMethodsOption: 'GET',
        pathOption: '',
        refererOption: ''
    };

    /**
     * The management service document for determining whether the operation
     * tracing is on or off
     */
    private _management: ServiceDocument;

    /**
     * Object used for query
     */
    private _traceTask: QueryTask;

    /**
     * Object contains both the original query and the result
     */
    private _traceResult: QueryTask;

    /**
     * Subscriptions to services.
     */
    private _baseServiceManagementSubscription: Subscription;
    private _baseServiceManagementPatchSubscription: Subscription;
    private _baseServiceQuerySubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService) {}

    ngOnInit(): void {
        this._baseServiceManagementSubscription =
            this._baseService.getDocument(URL.CoreManagement).subscribe(
                (management: ServiceDocument) => {
                    this._management = management;

                    if (this._management.hasOwnProperty('operationTracingLevel')
                            && this._management['operationTracingLevel'] === 'ALL') {
                        this.isRecordingOn = true;
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve operation tracing recording status: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceManagementSubscription)) {
            this._baseServiceManagementSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceManagementPatchSubscription)) {
            this._baseServiceManagementPatchSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceQuerySubscription)) {
            this._baseServiceQuerySubscription.unsubscribe();
        }
    }

    getTraceResult(): QueryTask | any {
        return this._traceResult ? this._traceResult : {};
    }

    getTraceResultCount(): number {
        if (_.isUndefined(this._traceResult) || _.isNull(this._traceResult)
            || _.isUndefined(this._traceResult.results) || _.isEmpty(this._traceResult.results)) {
            return 0;
        }

        if (_.isUndefined(this._traceResult.results.documentCount)) {
            return this._traceResult.results.documentLinks ?
                this._traceResult.results.documentLinks.length : 0;
        }

        return this._traceResult.results.documentCount;
    }

    onToggleTrace(event: MouseEvent): void {
        this.isRecordingOn = !this.isRecordingOn;

        // Do not subsribe since no body will be returned
        this._baseServiceManagementPatchSubscription =
            this._baseService.patch(URL.CoreManagement, {
                enable: this.isRecordingOn ? 'START' : 'STOP',
                kind: 'com:vmware:xenon:services:common:ServiceHostManagementService:ConfigureOperationTracingRequest'
            }).subscribe(() => {
                // Do nothing since no body will be returned.
            }, () => {
                // Do nothing since no body will be returned.
            });
    }

    onTraceMethodOptionChanged(event: MouseEvent) {
        this._traceOptions.traceMethodsOption = event.target['value'];
    }

    onTracePathOptionChanged(event: Event) {
        this._traceOptions.pathOption = event.target['value'];
    }

    onTraceRefererOptionChanged(event: Event) {
        this._traceOptions.refererOption = event.target['value'];
    }

    onAddClause(event: MouseEvent): void {
        this.traceClauses.push({
            id: ++this._clauseIdTracker
        });
    }

    onDeleteClause(context: EventContext): void {
        var id = context.data['id'];

        _.remove(this.traceClauses, (clause: any) => {
            return id === clause.id;
        });

        if (this.traceClauses.length === 0) {
            this._clauseIdTracker = 0;
        }
    }

    onClearTrace(event: MouseEvent): void {
        this._clauseIdTracker = 0;
        this.traceClauses = [];

        this._traceResult = null;
    }

    onTrace(event: MouseEvent): void {
        try {
            // Reset
            this._traceResult = null;
            this._traceTask = null;

            this._traceTask = this._buildInteractiveQueryTask();

            if (_.isUndefined(this._traceTask) || _.isNull(this._traceTask)
                    || _.isEmpty(this._traceTask)) {
                throw new Error('The query is empty');
            }

            this._baseServiceQuerySubscription = this._baseService.post(URL.Query, this._traceTask).subscribe(
                (result: QueryTask) => {
                    this._traceResult = result;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to query: [${error.statusCode}] ${error.message}`]
                    }]);
                });
        } catch (e) {
            this._notificationService.set([{
                type: 'ERROR',
                messages: [`Invalid query: ${e}`]
            }]);
        }
    }

    private _buildInteractiveQueryTask(): QueryTask {
        // Build specs
        var spec: QuerySpecification = {
            options: ['EXPAND_CONTENT'],
            query: {
                booleanClauses: [{
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'referer',
                        matchValue: this._traceOptions.refererOption,
                        matchType: 'WILDCARD'
                    }
                }, {
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'action',
                        matchValue: this._traceOptions.traceMethodsOption,
                        matchType: 'TERM'
                    }
                }, {
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'path',
                        matchValue: this._traceOptions.pathOption,
                        matchType: 'WILDCARD'
                    }
                }],
                occurance: 'MUST_OCCUR'
            }
        };

        this.operationTracingClauseComponents.forEach(
                (operationTracingClauseComponent: OperationTracingClauseComponent) => {
            // For sorting clause it's not a booleanClause
            var clause: BooleanClause | any = operationTracingClauseComponent.getClause();

            // For regular clauses
            spec.query.booleanClauses.push(clause);
        });

        return {
            taskInfo: {
                isDirect: true // always make sure it's direct, for UI
            },
            querySpec: spec,
            indexLink: '/core/operation-index'
        };
    }
}
