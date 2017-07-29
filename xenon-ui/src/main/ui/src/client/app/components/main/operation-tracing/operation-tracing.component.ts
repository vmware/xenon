// angular
import { Component, OnInit, OnDestroy, QueryList, ViewChildren } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { BooleanClause, EventContext, Node, QuerySpecification, QueryTask,
    ServiceDocument } from '../../../modules/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../modules/app/services/index';

import { OperationTracingClauseComponent } from './operation-tracing-clause.component';

@Component({
    selector: 'xe-operation-tracing',
    moduleId: module.id,
    templateUrl: './operation-tracing.component.html',
    styleUrls: ['./operation-tracing.component.css']
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
    private clauseIdTracker: number = 0;

    /**
     * Track the query options in interactive mode
     */
    private traceOptions: any = {
        traceMethodsOption: 'GET',
        pathOption: '',
        refererOption: ''
    };

    /**
     * The management service document for determining whether the operation
     * tracing is on or off
     */
    private management: ServiceDocument;

    /**
     * Object used for query
     */
    private traceTask: QueryTask;

    /**
     * Object contains both the original query and the result
     */
    private traceResult: QueryTask;

    /**
     * Subscriptions to services.
     */
    private baseServiceManagementSubscription: Subscription;
    private baseServiceManagementPatchSubscription: Subscription;
    private baseServiceQuerySubscription: Subscription;
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
        if (!_.isUndefined(this.baseServiceManagementSubscription)) {
            this.baseServiceManagementSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceManagementPatchSubscription)) {
            this.baseServiceManagementPatchSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceQuerySubscription)) {
            this.baseServiceQuerySubscription.unsubscribe();
        }
    }

    getTraceResult(): QueryTask | any {
        return this.traceResult ? this.traceResult : {};
    }

    getTraceResultCount(): number {
        if (_.isUndefined(this.traceResult) || _.isNull(this.traceResult)
            || _.isUndefined(this.traceResult.results) || _.isEmpty(this.traceResult.results)) {
            return 0;
        }

        if (_.isUndefined(this.traceResult.results.documentCount)) {
            return this.traceResult.results.documentLinks ?
                this.traceResult.results.documentLinks.length : 0;
        }

        return this.traceResult.results.documentCount;
    }

    onToggleTrace(event: MouseEvent): void {
        this.isRecordingOn = !this.isRecordingOn;

        // Do not subsribe since no body will be returned
        this.baseServiceManagementPatchSubscription =
            this.baseService.patch(URL.CoreManagement, {
                enable: this.isRecordingOn ? 'START' : 'STOP',
                kind: 'com:vmware:xenon:services:common:ServiceHostManagementService:ConfigureOperationTracingRequest'
            }).subscribe(() => {
                // Do nothing since no body will be returned.
            }, () => {
                // Do nothing since no body will be returned.
            });
    }

    onTraceMethodOptionChanged(event: MouseEvent) {
        this.traceOptions.traceMethodsOption = event.target['value'];
    }

    onTracePathOptionChanged(event: Event) {
        this.traceOptions.pathOption = event.target['value'];
    }

    onTraceRefererOptionChanged(event: Event) {
        this.traceOptions.refererOption = event.target['value'];
    }

    onAddClause(event: MouseEvent): void {
        this.traceClauses.push({
            id: ++this.clauseIdTracker
        });
    }

    onDeleteClause(context: EventContext): void {
        var id = context.data['id'];

        _.remove(this.traceClauses, (clause: any) => {
            return id === clause.id;
        });

        if (this.traceClauses.length === 0) {
            this.clauseIdTracker = 0;
        }
    }

    onClearTrace(event: MouseEvent): void {
        this.clauseIdTracker = 0;
        this.traceClauses = [];

        this.traceResult = null;
    }

    onTrace(event: MouseEvent): void {
        try {
            // Reset
            this.traceResult = null;
            this.traceTask = null;

            this.traceTask = this.buildInteractiveQueryTask();

            if (_.isUndefined(this.traceTask) || _.isNull(this.traceTask)
                    || _.isEmpty(this.traceTask)) {
                throw new Error('The query is empty');
            }

            this.baseServiceQuerySubscription = this.baseService.post(URL.QueryLocal, this.traceTask).subscribe(
                (result: QueryTask) => {
                    this.traceResult = result;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to query: [${error.statusCode}] ${error.message}`]
                    }]);
                });
        } catch (e) {
            this.notificationService.set([{
                type: 'ERROR',
                messages: [`Invalid query: ${e}`]
            }]);
        }
    }

    private buildInteractiveQueryTask(): QueryTask {
        // Build specs
        var spec: QuerySpecification = {
            options: ['EXPAND_CONTENT'],
            query: {
                booleanClauses: [{
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'referer',
                        matchValue: this.traceOptions.refererOption,
                        matchType: 'WILDCARD'
                    }
                }, {
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'action',
                        matchValue: this.traceOptions.traceMethodsOption,
                        matchType: 'TERM'
                    }
                }, {
                    occurance: 'MUST_OCCUR',
                    term: {
                        propertyName: 'path',
                        matchValue: this.traceOptions.pathOption,
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

    private getData(): void {
        this.baseServiceManagementSubscription =
            this.baseService.getDocument(URL.CoreManagement).subscribe(
                (management: ServiceDocument) => {
                    this.management = management;

                    if (this.management.hasOwnProperty('operationTracingLevel')
                            && this.management['operationTracingLevel'] === 'ALL') {
                        this.isRecordingOn = true;
                    } else {
                        this.isRecordingOn = false;
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve operation tracing recording status: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
