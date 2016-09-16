// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { QueryClauseComponent } from './query-clause.component';

// import { URL } from '../../../frameworks/app/enums/index';
import { EventContext, Node } from '../../../frameworks/app/interfaces/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-query',
    moduleId: module.id,
    templateUrl: './query.component.html',
    styleUrls: ['./query.component.css'],
    directives: [QueryClauseComponent],
    changeDetection: ChangeDetectionStrategy.Default
})

export class QueryComponent implements OnInit, OnDestroy {
    queryBuilderMode: string = 'interactive';

    querySpec: any = {
    };

    clauses: any[] = [{
        id: 1
    }];

    /**
     * Track the id of the next clause to be added. It starts from 1
     * and will always increase even when some clauses are deleted along
     * the way.
     */
    private _clauseIdTracker: number = 1;

    /**
     * Subscriptions to services.
     */
    private _baseServiceSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService) {}

    ngOnInit(): void {
        this._getData();

        // Update data when selected node changes
        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceSubscription)) {
            this._baseServiceSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    onQueryBuilderModeChanged(event: MouseEvent, mode: string): void {
        event.stopPropagation();

        this.queryBuilderMode = mode;
    }

    onAddClause(event: MouseEvent): void {
        this.clauses.push({
            id: ++this._clauseIdTracker
        });
    }

    onDeleteClause(context: EventContext): void {
        var id = context.data['id'];

        _.remove(this.clauses, (clause: any) => {
            return id === clause.id;
        });
    }

    private _getData(): void {
        // Do nothing
    }
}
