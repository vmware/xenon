// angular
import { ChangeDetectionStrategy, OnDestroy, QueryList, ViewChildren } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';
import { URL } from '../../../frameworks/app/enums/index';
import { BooleanClause, EventContext, QuerySpecification, QueryTask } from '../../../frameworks/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

import { QueryClauseComponent } from './query-clause.component';

/**
 * Query task snippet to be used in advanced query input as sample
 */
const sampleQueryTask: string = `{
    "taskInfo": {
        "isDirect": true // No result will be shown if set to false
    },
    "querySpec": {
        "options": [
            // Query Options
        ],
        "query": {
            "occurance": "MUST_OCCUR", // Occurance Options
            "term": {
                "matchType": "WILDCARD", // Term Matching Options
                "matchValue": "service-*",
                "propertyName": "name"
            }
        },
        "sortTerm" : {
            "propertyType": "STRING",
            "propertyName": "name"
        },
        "sortOrder" : "DESC"
    }
}`;

@BaseComponent({
    selector: 'xe-query',
    moduleId: module.id,
    templateUrl: './query.component.html',
    styleUrls: ['./query.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class QueryComponent implements OnDestroy {
    @ViewChildren(QueryClauseComponent)
    queryClauseComponents: QueryList<QueryClauseComponent>;

    /**
     * Two modes are possible for creating a query: 'interactive' and 'advanced'.
     */
    queryBuilderMode: string = 'interactive';

    /**
     * Track the clauses created or removed in interactive mode.
     * NOTE: It does not represent the actual data in the clauses
     */
    interactiveQueryClauses: any[] = [{
        id: 1
    }];

    /**
     * Track the content typed in the code editor in advanced mode.
     */
    advancedQueryTaskInput: string = sampleQueryTask;

    /**
     * Track the id of the next clause to be added. It starts from 1
     * and will always increase even when some clauses are deleted along
     * the way.
     */
    private _clauseIdTracker: number = 1;

    /**
     * Track the query options in interactive mode
     */
    private _interactiveQueryOptions: any = {
        queryVersionsOption: '',
        deletedDocumentInclusionOption: '',
        ownerSelectionOption: ''
    };

    /**
     * Object used for query
     */
    private _queryTask: QueryTask;

    /**
     * Object contains both the original query and the result
     */
    private _queryResult: QueryTask;

    /**
     * Link to the selected result.
     */
    private _selectedResultDocumentLink: string = '';

    /**
     * Subscriptions to services.
     */
    private _baseServiceSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService) {}

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceSubscription)) {
            this._baseServiceSubscription.unsubscribe();
        }
    }

    getQueryResult(): QueryTask | any {
        return this._queryResult ? this._queryResult : {};
    }

    getQueryResultCount(): number {
        if (_.isUndefined(this._queryResult) || _.isNull(this._queryResult)
            || _.isUndefined(this._queryResult.results) || _.isEmpty(this._queryResult.results)) {
            return 0;
        }

        if (_.isUndefined(this._queryResult.results.documentCount)) {
            return this._queryResult.results.documentLinks ?
                this._queryResult.results.documentLinks.length : 0;
        }

        return this._queryResult.results.documentCount;
    }

    getSelectedResultDocumentLink(): string {
        let link: string = this._selectedResultDocumentLink;

        // Process the selected result document link and make it a valid
        // url query parameter, for INCLUDE_ALL_VERSIONS case
        if (this._queryTask && this._queryTask.querySpec
                && this._queryTask.querySpec.options.indexOf('INCLUDE_ALL_VERSIONS') !== -1
                && link.indexOf('?documentVersion') !== -1) {
            link = link.replace('?', '&');
        }

        return link;
    }

    isResultLinkSelected(queryResultLink: string): boolean {
        return this._selectedResultDocumentLink === queryResultLink;
    }

    onQueryBuilderModeChanged(event: MouseEvent, mode: string): void {
        event.stopPropagation();

        this.queryBuilderMode = mode;
    }

    onQueryVersionsOptionChanged(event: MouseEvent): void {
        this._interactiveQueryOptions.queryVersionsOption = event.target['value'];
    }

    onDeletedDocumentInclusionOptionChanged(event: MouseEvent): void {
        this._interactiveQueryOptions.deletedDocumentInclusionOption = event.target['value'];
    }

    onOwnerSelectionOptionChanged(event: MouseEvent): void {
        this._interactiveQueryOptions.ownerSelectionOption = event.target['value'];
    }

    onAddClause(event: MouseEvent): void {
        this.interactiveQueryClauses.push({
            id: ++this._clauseIdTracker
        });
    }

    onDeleteClause(context: EventContext): void {
        var id = context.data['id'];

        _.remove(this.interactiveQueryClauses, (clause: any) => {
            return id === clause.id;
        });
    }

    onClearQuery(event: MouseEvent): void {
        if (this.queryBuilderMode === 'interactive') {
            this._clauseIdTracker = 1;
            this.interactiveQueryClauses = [{ id: 1 }];
        } else if (this.queryBuilderMode === 'advanced') {
            this.advancedQueryTaskInput = '';
        }

        this._queryResult = null;
    }

    onQuery(event: MouseEvent): void {
        try {
            // Reset
            this._selectedResultDocumentLink = '';
            this._queryResult = null;
            this._queryTask = null;

            if (this.queryBuilderMode === 'interactive') {
                this._queryTask = this._buildInteractiveQueryTask();
            } else if (this.queryBuilderMode === 'advanced') {
                this._queryTask = this._buildAdvancedQueryTask();
            }

            if (_.isUndefined(this._queryTask) || _.isNull(this._queryTask)
                    || _.isEmpty(this._queryTask)) {
                throw new Error('The query is empty');
            }

            this._baseServiceSubscription = this._baseService.post(URL.Query, this._queryTask).subscribe(
                (result: QueryTask) => {
                    this._queryResult = result;
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

    onSelectQueryResult(event: MouseEvent, queryResultLink: string): void {
        this._selectedResultDocumentLink = queryResultLink;
    }

    private _getQueryOptions(): string[] {
        var options: string[] = [];

        if (this._interactiveQueryOptions.queryVersionsOption) {
            options.push('INCLUDE_ALL_VERSIONS');
        }

        if (this._interactiveQueryOptions.deletedDocumentInclusionOption) {
            options.push('INCLUDE_DELETED');
        }

        if (this._interactiveQueryOptions.ownerSelectionOption) {
            options.push('OWNER_SELECTION');
        }

        return options;
    }

    private _buildInteractiveQueryTask(): QueryTask {
        // Build specs
        var spec: QuerySpecification = {
            options: this._getQueryOptions(),
            query: {
                booleanClauses: [],
                occurance: 'MUST_OCCUR'
            }
        };
        this.queryClauseComponents.forEach((queryClauseComponent: QueryClauseComponent) => {
            var clauseType: string = queryClauseComponent.type;

            // For sorting clause it's not a booleanClause
            var clause: BooleanClause | any = queryClauseComponent.getClause();

            // Special handling for sort
            if (clauseType === 'sort') {
                spec.options.push('SORT');
                spec.sortTerm = {
                    propertyName: clause.sortTerm,
                    propertyType: 'STRING'
                };
                spec.sortOrder = clause.sortOrder;
                return;
            }

            // Special handling for group
            if (clauseType === 'group') {
                spec.options.push('GROUP_BY');
                // Required to have both 'groupByTerm' and 'groupSortTerm'
                // in place in order to do group sort property
                spec.groupByTerm = clause;
                spec.groupSortTerm = clause;
                return;
            }

            // For regular clauses
            spec.query.booleanClauses.push(clause);
        });

        return {
            taskInfo: {
                isDirect: true // always make sure it's direct, for UI
            },
            querySpec: spec
        };
    }

    private _buildAdvancedQueryTask(): QueryTask {
        return JSON.parse(this.advancedQueryTaskInput) as QueryTask;
    }
}
