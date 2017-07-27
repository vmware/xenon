// angular
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';
// import * as _ from 'lodash';

// app
import { BooleanClause, EventContext, NumericRange } from '../../../modules/app/interfaces/index';

@Component({
    selector: 'xe-query-clause-nested',
    moduleId: module.id,
    templateUrl: './query-clause-nested.component.html',
    styleUrls: ['./query-clause-nested.component.css'],
    changeDetection: ChangeDetectionStrategy.OnPush
})

export class QueryClauseNestedComponent {
    /**
     * Uniqie ID for the clause
     */
    @Input()
    id: number;

    /**
     * Type of the clause, possible values: 'string', 'range'
     */
    @Input()
    type: string = 'string';

    /**
     * Whether this clause is the first one
     */
    @Input()
    isFirst: boolean = false;

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
    propertyMatchType: string = 'WILDCARD';

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

    getClause(): BooleanClause {
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
