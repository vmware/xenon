// angular
import { Component, Input,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { ServiceDocumentQueryResult } from '../../../modules/app/interfaces/index';
import { StringUtil } from '../../../modules/app/utils/index';
import { BaseService, NotificationService } from '../../../modules/app/services/index';

@Component({
    selector: 'xe-query-result-detail',
    moduleId: module.id,
    templateUrl: './query-result-detail.component.html',
    styleUrls: ['./query-result-detail.component.css']
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
    private selectedResultDocument: ServiceDocumentQueryResult;

    /**
     * Subscriptions to services.
     */
    private baseServiceGetResultSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var selectedResultDocumentLinkChanges = changes['selectedResultDocumentLink'];

        if (!selectedResultDocumentLinkChanges
            || _.isEqual(selectedResultDocumentLinkChanges.currentValue, selectedResultDocumentLinkChanges.previousValue)
            || _.isEmpty(selectedResultDocumentLinkChanges.currentValue)) {
            return;
        }

        this.selectedResultDocumentLink = selectedResultDocumentLinkChanges.currentValue;

        this.getData();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceGetResultSubscription)) {
            this.baseServiceGetResultSubscription.unsubscribe();
        }
    }

    getSelectedResultDocument(): ServiceDocumentQueryResult {
        return this.selectedResultDocument;
    }

    getTimeStamp(timeMicros: number): string {
        return StringUtil.getTimeStamp(timeMicros);
    }

    isNullOrUndefined(value: any) {
        return _.isNull(value) || _.isUndefined(value);
    }

    private getData(): void {
        let link: string = `?documentSelfLink=${this.selectedResultDocumentLink}`;

        this.baseServiceGetResultSubscription =
            this.baseService.getDocument(URL.DocumentIndex + link).subscribe(
                (childService: ServiceDocumentQueryResult) => {
                    this.selectedResultDocument = childService;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve details for the selected query result: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
