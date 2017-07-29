// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Location } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { ModalContext, Node, QueryTask, ServiceDocument,
    ServiceDocumentQueryResult } from '../../../modules/app/interfaces/index';
import { ODataUtil, StringUtil } from '../../../modules/app/utils/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../modules/app/services/index';

@Component({
    selector: 'xe-service-detail',
    moduleId: module.id,
    templateUrl: './service-detail.component.html',
    styleUrls: ['./service-detail.component.css']
})

export class ServiceDetailComponent implements OnInit, OnDestroy {
    /**
     * Context object for rendering create child service modal.
     */
    createChildServiceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: '',
            body: ''
        }
    };

    /**
     * Links to all the available services.
     */
    private serviceLinks: string[] = [];

    /**
     * Links to all the available child services within the specified service.
     */
    private childServicesLinks: string[] = [];

    /**
     * Link to the next page of the child services
     */
    private childServiceNextPageLink: string;

    /**
     * A lock to prevent the same next page link from being requested multiple times when scrolling
     */
    private childServiceNextPageLinkRequestLocked: boolean = false;

    /**
     * Id for the selected service. E.g. /core/examples
     */
    private selectedServiceId: string;

    /**
     * Id for the selected child service.
     */
    private selectedChildServiceId: string;

    /**
     * Subscriptions to services.
     */
    private activatedRouteParamsSubscription: Subscription;
    private nodeSelectorServiceGetSelectedSubscription: Subscription;
    private baseServiceGetLinksSubscription: Subscription;
    private baseServiceGetChildServiceListSubscription: Subscription;
    private baseServiceGetChildServiceListNextPageSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private nodeSelectorService: NodeSelectorService,
        private notificationService: NotificationService,
        private activatedRoute: ActivatedRoute,
        private router: Router,
        private browserLocation: Location) {}

    ngOnInit(): void {
        // Update data when selected node changes
        this.nodeSelectorServiceGetSelectedSubscription =
            this.nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    // Navigate to the parent service grid when selected node changes
                    this.router.navigate(['/main/service'], {
                        relativeTo: this.activatedRoute,
                        queryParams: {
                            'node': this.activatedRoute.snapshot.queryParams['node']
                        }
                    });
                });

        this.activatedRouteParamsSubscription =
            this.activatedRoute.params.subscribe(
                (params: {[key: string]: any}) => {
                    this.selectedServiceId =
                        StringUtil.decodeFromId(params['id'] as string);

                    this.selectedChildServiceId = params['childId'];

                    // Set modal context
                    this.createChildServiceModalContext.name = this.selectedServiceId;
                    this.createChildServiceModalContext.data['documentSelfLink'] = this.selectedServiceId;
                    this.createChildServiceModalContext.data['body'] = '';

                    this.getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceGetLinksSubscription)) {
            this.baseServiceGetLinksSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetChildServiceListSubscription)) {
            this.baseServiceGetChildServiceListSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetSelectedSubscription)) {
            this.nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.activatedRouteParamsSubscription)) {
            this.activatedRouteParamsSubscription.unsubscribe();
        }
    }

    getServiceLinks(): string[] {
        return this.serviceLinks;
    }

    getChildServiceLinks(): string[] {
        return _.map(this.childServicesLinks, (childServiceLink: string) => {
            return StringUtil.parseDocumentLink(childServiceLink, this.selectedServiceId).id;
        });
    }

    getSelectedServiceId(): string {
        return this.selectedServiceId;
    }

    getSelectedServiceRouterId(id: string): string {
        return StringUtil.encodeToId(id);
    }

    getSelectedChildServiceId(): string {
        return this.selectedChildServiceId;
    }

    onCreateChildService(event: MouseEvent): void {
        var selectedServiceId: string = this.createChildServiceModalContext.data['documentSelfLink'];
        var body: string = this.createChildServiceModalContext.data['body'];

        if (!selectedServiceId || !body) {
            return;
        }

        this.baseService.post(selectedServiceId, body).subscribe(
            (document: ServiceDocument) => {
                this.notificationService.set([{
                    type: 'SUCCESS',
                    messages: [`Child Service ${document.documentSelfLink} Created`]
                }]);

                // Reset body
                this.createChildServiceModalContext.data['body'] = '';
            },
            (error) => {
                // TODO: Better error handling
                this.notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    onSelectChildService(event: MouseEvent, childServiceId: string): void {
        event.stopImmediatePropagation();

        // Manually update the url to represent the selected child service instead of
        // using router navigate, to prevent the page from flashing (thus all the child services
        // get reloaded and pagination get massed up), and offer better performance.
        var serviceId: string = this.activatedRoute.snapshot.params['id'];
        var basePath = this.browserLocation.path();

        if (_.isUndefined(this.selectedChildServiceId)) {
            this.browserLocation.replaceState(basePath.replace(serviceId, `${serviceId}/${childServiceId}`));
        } else {
            this.browserLocation.replaceState(basePath.replace(this.selectedChildServiceId, childServiceId));
        }

        this.selectedChildServiceId = childServiceId;
    }

    onLoadNextPage(): void {
        if (_.isUndefined(this.childServiceNextPageLink) || this.childServiceNextPageLinkRequestLocked) {
            return;
        }

        this.childServiceNextPageLinkRequestLocked = true;
        this.baseServiceGetChildServiceListNextPageSubscription =
            this.baseService.getDocument(this.childServiceNextPageLink, '', false).subscribe(
                (document: QueryTask) => {
                    if (_.isEmpty(document.results) || this.childServiceNextPageLink === document.results.nextPageLink) {
                        return;
                    }

                    this.childServiceNextPageLinkRequestLocked = false;
                    this.childServicesLinks = _.concat(this.childServicesLinks, document.results.documentLinks);

                    // NOTE: Need to use forwarding link here since the paginated data is stored on a particular node
                    if (document.results.nextPageLink) {
                        this.childServiceNextPageLink = this.baseService.getForwardingLink(document.results.nextPageLink);
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    private getData(): void {
        // Only get serviceLinks once
        if (_.isEmpty(this.serviceLinks)) {
            // Reset childServicesLinks when the service itself changes
            this.childServicesLinks = [];

            this.baseServiceGetLinksSubscription =
                this.baseService.post(URL.Root, URL.RootPostBody).subscribe(
                    (document: ServiceDocumentQueryResult) => {
                        this.serviceLinks = _.sortBy(document.documentLinks);
                    },
                    (error) => {
                        // TODO: Better error handling
                        this.notificationService.set([{
                            type: 'ERROR',
                            messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                        }]);
                    });
        }

        this.baseServiceGetChildServiceListSubscription =
            this.baseService.getDocument(this.selectedServiceId, `${ODataUtil.limit()}&${ODataUtil.orderBy('documentSelfLink')}`).subscribe(
                (document: ServiceDocumentQueryResult) => {
                    this.childServicesLinks = document.documentLinks;
                    console.log(document);

                    // NOTE: Need to use forwarding link here since the paginated data is stored on a particular node
                    if (document.nextPageLink) {
                        this.childServiceNextPageLink = this.baseService.getForwardingLink(document.nextPageLink);
                    }
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
