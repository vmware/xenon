// angular
import { ChangeDetectionStrategy, Component, QueryList, ViewChildren } from '@angular/core';
import * as _ from 'lodash';

// app
import { BooleanClause, EventContext } from '../../../modules/app/interfaces/index';

import { QueryClauseNestedComponent } from './query-clause-nested.component';

@Component({
    selector: 'xe-query-nested',
    moduleId: module.id,
    templateUrl: './query-nested.component.html',
    styleUrls: ['./query-nested.component.css'],
    changeDetection: ChangeDetectionStrategy.OnPush
})

export class QueryNestedComponent {
    @ViewChildren(QueryClauseNestedComponent)
    queryClauseNestedComponents: QueryList<QueryClauseNestedComponent>;

    /**
     * Track the nested clauses created or removed in the component.
     * NOTE: It does not represent the actual data in the clauses
     */
    clauses: any[] = [{
        id: 1
    }];

    /**
     * Track the id of the next clause to be added. It starts from 1
     * and will always increase even when some clauses are deleted along
     * the way.
     */
    private clauseIdTracker: number = 1;

    getClauses(): BooleanClause[] {
        var booleanClauses: BooleanClause[] = [];

        this.queryClauseNestedComponents.forEach((queryClauseComponent: QueryClauseNestedComponent) => {
            booleanClauses.push(queryClauseComponent.getClause());
        });

        return booleanClauses;
    }

    onAddClause(event: MouseEvent): void {
        this.clauses.push({
            id: ++this.clauseIdTracker
        });
    }

    onDeleteClause(context: EventContext): void {
        var id = context.data['id'];

        _.remove(this.clauses, (clause: any) => {
            return id === clause.id;
        });
    }
}
