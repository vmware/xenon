// angular
import { ChangeDetectionStrategy, Input,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { URL } from '../../../frameworks/app/enums/index';
import { ServiceDocumentQueryResult } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-query-result-detail',
    moduleId: module.id,
    templateUrl: './query-result-detail.component.html',
    styleUrls: ['./query-result-detail.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class QueryResultDetailComponent implements OnChanges, OnDestroy {
    /**
     * Link to the selected document. E.g. /core/examples/xxx
     */
    @Input()
    selectedResultDocumentLink: string;

    /**
     * Selected query result document
     */
    private _selectedResultDocument: ServiceDocumentQueryResult;

    /**
     * Subscriptions to services.
     */
    private _baseServiceGetResultSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var selectedResultDocumentLinkChanges = changes['selectedResultDocumentLink'];

        if (!selectedResultDocumentLinkChanges
            || _.isEqual(selectedResultDocumentLinkChanges.currentValue, selectedResultDocumentLinkChanges.previousValue)
            || _.isEmpty(selectedResultDocumentLinkChanges.currentValue)) {
            return;
        }

        this.selectedResultDocumentLink = selectedResultDocumentLinkChanges.currentValue;

        this._getData();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceGetResultSubscription)) {
            this._baseServiceGetResultSubscription.unsubscribe();
        }
    }

    getSelectedResultDocument(): ServiceDocumentQueryResult {
        return this._selectedResultDocument;
    }

    getTimeStamp(timeMicros: number): string {
        return StringUtil.getTimeStamp(timeMicros);
    }

    isNullOrUndefined(value: any) {
        return _.isNull(value) || _.isUndefined(value);
    }

    private _getData(): void {
        let link: string = `?documentSelfLink=${this.selectedResultDocumentLink}`;

        this._baseServiceGetResultSubscription =
            this._baseService.getDocument(URL.DocumentIndex + link).subscribe(
                (childService: ServiceDocumentQueryResult) => {
                    this._selectedResultDocument = childService;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve details for the selected query result: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
