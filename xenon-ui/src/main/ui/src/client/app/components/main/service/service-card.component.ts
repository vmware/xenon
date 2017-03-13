// angular
import { ChangeDetectionStrategy, EventEmitter, Input, Output,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { EventContext, ServiceConfiguration, ServiceDocumentQueryResult,
    ServiceStats } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-service-card',
    moduleId: module.id,
    templateUrl: './service-card.component.html',
    styleUrls: ['./service-card.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ServiceCardComponent implements OnChanges, OnDestroy {
    /**
     * The service in the view.
     */
    @Input()
    service: ServiceDocumentQueryResult;

    /**
     * Emits the context when the create child service button is clicked.
     */
    @Output()
    createChildServiceClicked = new EventEmitter<EventContext>();

    /**
     * Flag indicating whether the service is currently available.
     */
    private _isServiceAvailable: boolean = false;

    /**
     * Stats of the service
     */
    private _serviceStats: ServiceStats;

    /**
     * Config of the service
     */
    private _serviceConfig: ServiceConfiguration;

    /**
     * Subscriptions to services.
     */
    private _baseServiceStatsSubscription: Subscription;
    private _baseServiceConfigSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var serviceChanges = changes['service'];

        if (!serviceChanges
            || _.isEqual(serviceChanges.currentValue, serviceChanges.previousValue)
            || _.isEmpty(serviceChanges.currentValue)) {
            return;
        }

        this.service = serviceChanges.currentValue;

        this._getData();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceStatsSubscription)) {
            this._baseServiceStatsSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceConfigSubscription)) {
            this._baseServiceConfigSubscription.unsubscribe();
        }
    }

    getService(): string {
        return this.service ? this.service.documentSelfLink : '';
    }

    getServiceAvailabilityTitle(): string {
        var availability: string = this._isServiceAvailable ? 'AVAILABLE' : 'UNAVAILABLE';
        return `Status: ${availability}`;
    }

    getServiceAvailabilityClass(baseClasses: string): string {
        if (_.isEmpty(this.service)) {
            return baseClasses;
        }

        var statusClass: string = this._isServiceAvailable ? 'available-status' : 'error-status';
        return `${baseClasses} ${statusClass}`;
    }

    getChildServiceCount(): number {
        if (_.isEmpty(this.service)) {
            return 0;
        }

        if (_.isUndefined(this.service.documentCount)) {
            return this.service.documentLinks ? this.service.documentLinks.length : 0;
        }

        return this.service.documentCount;
    }

    getServiceConfiguration(): ServiceConfiguration {
        return this._serviceConfig;
    }

    getRouterId(id: string) {
        return StringUtil.encodeToId(id);
    }

    getTimeStamp(timeMicros: number): string {
        return StringUtil.getTimeStamp(timeMicros);
    }

    onCreateChildServiceClicked(event: MouseEvent): void {
        var context: EventContext = {
            type: event.type,
            data: {
                documentSelfLink: this.service ? this.service.documentSelfLink : ''
            }
        };
        this.createChildServiceClicked.emit(context);
    }

    private _getData(): void {
        this._baseServiceStatsSubscription =
            this._baseService.getDocumentStats(this.service.documentSelfLink).subscribe(
                (stats: ServiceStats) => {
                    this._serviceStats = stats;
                    this._isServiceAvailable = this._serviceStats.entries['isAvailable'] ?
                        stats.entries['isAvailable'].latestValue === 1 : false;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this._baseServiceConfigSubscription =
            this._baseService.getDocumentConfig(this.service.documentSelfLink).subscribe(
                (config: ServiceConfiguration) => {
                    this._serviceConfig = config;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
