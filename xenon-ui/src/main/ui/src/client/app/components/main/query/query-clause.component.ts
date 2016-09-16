// angular
import { ChangeDetectionStrategy, EventEmitter, Input, Output,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
// import { Subscription } from 'rxjs/Subscription';
// import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

// import { URL } from '../../../frameworks/app/enums/index';
import { EventContext } from '../../../frameworks/app/interfaces/index';
// import { BaseService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-query-clause',
    moduleId: module.id,
    templateUrl: './query-clause.component.html',
    styleUrls: ['./query-clause.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class QueryClauseComponent implements OnChanges, OnDestroy {
    @Input()
    id: number;

    @Input()
    isFirst: boolean = false;

    @Output()
    deleteClause = new EventEmitter<EventContext>();

    /**
     * Type of the clause, possible values: 'string', 'range', 'sort', 'nested'
     */
    type: string = 'string';

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        // do nothing
    }

    ngOnDestroy(): void {
        // do nothing
    }

    onQueryClauseTypeChanged(event: MouseEvent): void {
        this.type = event.target['value'];
    }

    onDeleteClauseClicked(event: MouseEvent): void {
        this.deleteClause.emit({
            type: event.type,
            data: {
                id: this.id
            }
        });
    }
}
