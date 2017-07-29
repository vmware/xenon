// angular
import { Component, EventEmitter, Input, Output,
    SimpleChange, OnChanges, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { EventContext, ServiceConfiguration, ServiceDocumentQueryResult,
    ServiceStats } from '../../../modules/app/interfaces/index';
import { StringUtil } from '../../../modules/app/utils/index';
import { BaseService, NotificationService } from '../../../modules/app/services/index';

@Component({
    selector: 'xe-service-card',
    moduleId: module.id,
    templateUrl: './service-card.component.html',
    styleUrls: ['./service-card.component.css']
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
    private isServiceAvailable: boolean = false;

    /**
     * Stats of the service
     */
    private serviceStats: ServiceStats;

    /**
     * Config of the service
     */
    private serviceConfig: ServiceConfiguration;

    /**
     * Subscriptions to services.
     */
    private baseServiceStatsSubscription: Subscription;
    private baseServiceConfigSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private notificationService: NotificationService) {}

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var serviceChanges = changes['service'];

        if (!serviceChanges
            || _.isEqual(serviceChanges.currentValue, serviceChanges.previousValue)
            || _.isEmpty(serviceChanges.currentValue)) {
            return;
        }

        this.service = serviceChanges.currentValue;

        this.getData();
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceStatsSubscription)) {
            this.baseServiceStatsSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceConfigSubscription)) {
            this.baseServiceConfigSubscription.unsubscribe();
        }
    }

    getService(): string {
        return this.service ? this.service.documentSelfLink : '';
    }

    getServiceAvailabilityTitle(): string {
        var availability: string = this.isServiceAvailable ? 'AVAILABLE' : 'UNAVAILABLE';
        return `Status: ${availability}`;
    }

    getServiceAvailabilityClass(baseClasses: string): string {
        if (_.isEmpty(this.service)) {
            return baseClasses;
        }

        var statusClass: string = this.isServiceAvailable ? 'available-status' : 'error-status';
        return `${baseClasses} ${statusClass}`;
    }

    getChildServiceCount(): string {
        if (_.isEmpty(this.service)) {
            return '0';
        }

        if (_.isUndefined(this.service.documentCount)) {
            return this.service.documentLinks ? `${this.service.documentLinks.length}` : '0';
        }

        return StringUtil.formatNumber(this.service.documentCount, true);
    }

    getServiceConfiguration(): ServiceConfiguration {
        return this.serviceConfig;
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

    private getData(): void {
        this.baseServiceStatsSubscription =
            this.baseService.getDocumentStats(this.service.documentSelfLink).subscribe(
                (stats: ServiceStats) => {
                    this.serviceStats = stats;
                    this.isServiceAvailable = this.serviceStats.entries['isAvailable'] ?
                        stats.entries['isAvailable'].latestValue === 1 : false;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this.baseServiceConfigSubscription =
            this.baseService.getDocumentConfig(this.service.documentSelfLink).subscribe(
                (config: ServiceConfiguration) => {
                    this.serviceConfig = config;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
