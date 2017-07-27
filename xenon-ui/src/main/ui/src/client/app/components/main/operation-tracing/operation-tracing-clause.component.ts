// angular
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
// import * as _ from 'lodash';

// app
import { BooleanClause, EventContext, NumericRange } from '../../../modules/app/interfaces/index';

@Component({
    selector: 'xe-operation-tracing-clause',
    moduleId: module.id,
    templateUrl: './operation-tracing-clause.component.html',
    styleUrls: ['./operation-tracing-clause.component.css'],
    changeDetection: ChangeDetectionStrategy.OnPush
})

export class OperationTracingClauseComponent {
    /**
     * Uniqie ID for the clause
     */
    @Input()
    id: number;

    /**
     * Type of the clause, possible values: 'string', 'range', 'sort', 'group', 'nested'
     */
    @Input()
    type: string = 'string';

    /**
     * Whether this clause is the first one
     */
    @Input()
    isFirst: boolean = false;

    /**
     * Whether there are more than one clause
     */
    @Input()
    hasMoreThanOne: boolean = false;

    @Output()
    deleteClause = new EventEmitter<EventContext>();

    /**
     * Property match type, possible values: 'MUST_OCCUR' (equivalent of AND -- default),
     * 'MUST_NOT_OCCUR' (equivalent of NOT), 'SHOULD_OCCUR' (equivalent of OR)
     */
    occurance: string = 'MUST_OCCUR';

    /**
     * Property name to be matched/queried
     */
    propertyName: string = '';

    /**
     * Property match type, possible values: 'TERM', 'PHRASE', 'WILDCARD', 'PREFIX'
     */
    propertyMatchType: string = 'TERM';

    /**
     * Property value to be matched/queried
     */
    propertyValue: string = '';

    /**
     * In case of range query, the property value range to be matched/quried
     */
    propertyValueRange: NumericRange = {
        precisionStep: '16', // Can't change this right now
        isMaxInclusive: 'true',
        isMinInclusive: 'true',
        max: 0.0,
        min: 0.0,
        type: 'DOUBLE' // Can't change this right now
    };

    /**
     * Sort order, possible values: 'ASC', 'DESC'
     */
    sortOrder: string = 'ASC';

    getClause(): any {
        switch(this.type) {
            case 'string':
                return this.getStringClause();
            case 'range':
                return this.getRangeClause();
        }

        return null;
    }

    getAsNumber(value: string): number {
        return value ? parseFloat(value) : 0.0;
    }

    onDeleteClauseClicked(event: MouseEvent): void {
        this.deleteClause.emit({
            type: event.type,
            data: {
                id: this.id
            }
        });
    }

    private getStringClause(): BooleanClause {
        return {
            term: {
                matchType: this.propertyMatchType,
                matchValue: this.propertyValue,
                propertyName: this.propertyName
            },
            occurance: this.occurance
        };
    }

    private getRangeClause(): BooleanClause {
        return {
            term: {
                propertyName: this.propertyName,
                range: this.propertyValueRange
            },
            occurance: this.occurance
        };
    }
}
