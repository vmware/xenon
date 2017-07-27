// angular
import { Component, OnDestroy, QueryList, ViewChildren } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { BooleanClause, EventContext, QuerySpecification, QueryTask } from '../../../modules/app/interfaces/index';
import { BaseService, NotificationService } from '../../../modules/app/services/index';
import { ODataUtil } from '../../../modules/app/utils/index';

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
        "sortOrder" : "DESC",
        "resultLimit": 25
    }
}`;

@Component({
    selector: 'xe-query',
    moduleId: module.id,
    templateUrl: './query.component.html',
    styleUrls: ['./query.component.css']
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
     * Whether a query has happend.
     */
    isQueried: boolean = false;

    /**
     * Track the id of the next clause to be added. It starts from 1
     * and will always increase even when some clauses are deleted along
     * the way.
     */
    private clauseIdTracker: number = 1;

    /**
     * Track the query options in interactive mode
     */
    private interactiveQueryOptions: any = {
        queryVersionsOption: '',
        deletedDocumentInclusionOption: '',
        ownerSelectionOption: ''
    };

    /**
     * Object used for query
     */
    private queryTask: QueryTask;

    /**
     * Array contains the documentLinks of the result
     */
    private queryResultLinks: string[];

    /**
     * Keep track of the total count of the query result. Can't rely on queryResultLinks.length
     * since it's paginated.
     */
    private queryResultCount: number = 0;

    /**
     * Link to the selected result.
     */
    private selectedResultDocumentLink: string = '';

    /**
     * Link to the next page of the child services
     */
    private queryResultNextPageLink: string;

    /**
     * A lock to prevent the same next page link from being requested multiple times when scrolling
     */
    private requestResultNextPageLinkRequestLocked: boolean = false;

    /**
     * Subscriptions to services.
     */
    private baseServiceSubscription: Subscription;
    private baseServiceGetQueryResultNextPageSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private notificationService: NotificationService) {}

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceSubscription)) {
            this.baseServiceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetQueryResultNextPageSubscription)) {
            this.baseServiceGetQueryResultNextPageSubscription.unsubscribe();
        }
    }

    getQueryResultLinks(): string[] {
        return this.queryResultLinks;
    }

    getQueryResultCount(): number {
        return this.queryResultCount;
    }

    getSelectedResultDocumentLink(): string {
        var link: string = this.selectedResultDocumentLink;

        // Process the selected result document link and make it a valid
        // url query parameter, for INCLUDE_ALL_VERSIONS case
        if (this.queryTask && this.queryTask.querySpec
                && this.queryTask.querySpec.options.indexOf('INCLUDE_ALL_VERSIONS') !== -1
                && link.indexOf('?documentVersion') !== -1) {
            link = link.replace('?', '&');
        }

        return link;
    }

    isResultLinkSelected(queryResultLink: string): boolean {
        return this.selectedResultDocumentLink === queryResultLink;
    }

    onQueryBuilderModeChanged(event: MouseEvent, mode: string): void {
        event.stopPropagation();

        this.queryBuilderMode = mode;
    }

    onQueryVersionsOptionChanged(event: MouseEvent): void {
        this.interactiveQueryOptions.queryVersionsOption = event.target['value'];
    }

    onDeletedDocumentInclusionOptionChanged(event: MouseEvent): void {
        this.interactiveQueryOptions.deletedDocumentInclusionOption = event.target['value'];
    }

    onOwnerSelectionOptionChanged(event: MouseEvent): void {
        this.interactiveQueryOptions.ownerSelectionOption = event.target['value'];
    }

    onAddClause(event: MouseEvent): void {
        this.interactiveQueryClauses.push({
            id: ++this.clauseIdTracker
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
            this.clauseIdTracker = 1;
            this.interactiveQueryClauses = [{ id: 1 }];
        } else if (this.queryBuilderMode === 'advanced') {
            this.advancedQueryTaskInput = '';
        }

        // Reset all the states
        this.queryResultLinks = [];
        this.queryResultCount = 0;
        this.isQueried = false;
    }

    onQuery(event: MouseEvent): void {
        try {
            // Reset
            this.selectedResultDocumentLink = '';
            this.queryResultLinks = [];
            this.queryTask = undefined;

            if (this.queryBuilderMode === 'interactive') {
                this.queryTask = this.buildInteractiveQueryTask();
            } else if (this.queryBuilderMode === 'advanced') {
                this.queryTask = this.buildAdvancedQueryTask();
            }

            if (_.isUndefined(this.queryTask) || _.isEmpty(this.queryTask)) {
                throw new Error('The query is empty');
            }

            // Add a COUNT option to the spec to get count
            var queryCountTask: QueryTask = _.cloneDeep(this.queryTask);
            if (queryCountTask.querySpec.options) {
                queryCountTask.querySpec.options.push('COUNT');
            } else {
                queryCountTask.querySpec.options = ['COUNT'];
            }

            // Delete resultLimit for count quert since it's not necessary and will cause warnings in log
            delete queryCountTask.querySpec.resultLimit;

            this.baseServiceSubscription = this.baseService.post(URL.Query, queryCountTask).subscribe(
                (result: QueryTask) => {
                    this.isQueried = true;
                    this.queryResultCount = result.results.documentCount;

                    if (this.queryResultCount === 0) {
                        return;
                    }

                    // Only do query task if the count is not 0
                    this.baseService.post(URL.Query, this.queryTask).subscribe((result: QueryTask) => {
                            this.queryResultNextPageLink = result.results.nextPageLink;
                            if (_.isUndefined(this.queryResultNextPageLink)) {
                                return;
                            }

                            this.getNextPage();
                        },
                        (error) => {
                            // TODO: Better error handling
                            this.notificationService.set([{
                                type: 'ERROR',
                                messages: [`Failed to query: [${error.statusCode}] ${error.message}`]
                            }]);
                        });
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

    onSelectQueryResult(event: MouseEvent, queryResultLink: string): void {
        this.selectedResultDocumentLink = queryResultLink;
    }

    onLoadNextPage(): void {
        if (_.isUndefined(this.queryResultNextPageLink) || this.requestResultNextPageLinkRequestLocked) {
            return;
        }

        this.getNextPage();
    }

    private getQueryOptions(): string[] {
        var options: string[] = [];

        if (this.interactiveQueryOptions.queryVersionsOption) {
            options.push('INCLUDE_ALL_VERSIONS');
        }

        if (this.interactiveQueryOptions.deletedDocumentInclusionOption) {
            options.push('INCLUDE_DELETED');
        }

        if (this.interactiveQueryOptions.ownerSelectionOption) {
            options.push('OWNER_SELECTION');
        }

        return options;
    }

    private getNextPage(): void {
        this.requestResultNextPageLinkRequestLocked = true;
        this.baseServiceGetQueryResultNextPageSubscription =
            this.baseService.getDocument(this.queryResultNextPageLink, '', false).subscribe(
                (document: QueryTask) => {
                    if (_.isEmpty(document.results) || this.queryResultNextPageLink === document.results.nextPageLink) {
                        return;
                    }

                    this.requestResultNextPageLinkRequestLocked = false;
                    this.queryResultLinks = _.concat(this.queryResultLinks, document.results.documentLinks);

                    // NOTE: Need to use forwarding link here since the paginated data is stored on a particular node
                    if (document.results.nextPageLink) {
                        this.queryResultNextPageLink = this.baseService.getForwardingLink(document.results.nextPageLink);
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to query: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    private buildInteractiveQueryTask(): QueryTask {
        // Build specs
        var spec: QuerySpecification = {
            options: this.getQueryOptions(),
            resultLimit: ODataUtil.DEFAULT_PAGE_SIZE,
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

    private buildAdvancedQueryTask(): QueryTask {
        return JSON.parse(this.advancedQueryTaskInput) as QueryTask;
    }
}
