// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { URL } from '../../../frameworks/app/enums/index';
import { ModalContext, Node, ServiceDocument, ServiceDocumentQueryResult } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-service-detail',
    moduleId: module.id,
    templateUrl: './service-detail.component.html',
    styleUrls: ['./service-detail.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ServiceDetailComponent implements OnInit, OnDestroy {
    /**
     * Context object for rendering create instance modal.
     */
    createInstanceModalContext: ModalContext = {
        name: '',
        data: {
            documentSelfLink: '',
            body: ''
        }
    };

    /**
     * links to all the available services.
     */
    private _serviceLinks: string[] = [];

    /**
     * links to all the available instances within the specified service.
     */
    private _childServicesLinks: string[] = [];

    /**
     * Id for the selected service. E.g. /core/examples
     */
    private _selectedServiceId: string = '';

    /**
     * Id for the selected child service.
     */
    private _selectedChildServiceId: string = '';

    /**
     * Subscriptions to services.
     */
    private _activatedRouteParamsSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;
    private _baseServiceGetLinksSubscription: Subscription;
    private _baseServiceGetChildServiceListSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService,
        private _activatedRoute: ActivatedRoute,
        private _router: Router) {}

    ngOnInit(): void {
        // Update data when selected node changes
        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    // Navigate to the parent service grid when selected node changes
                    this._router.navigate(['/main/service'], {
                        relativeTo: this._activatedRoute,
                        queryParams: {
                            'node': this._activatedRoute.snapshot.queryParams['node']
                        }
                    });
                });

        this._activatedRouteParamsSubscription =
            this._activatedRoute.params.subscribe(
                (params: {[key: string]: any}) => {
                    this._selectedServiceId =
                        StringUtil.decodeFromId(params['id'] as string);

                    this._selectedChildServiceId = params['instanceId'];

                    // Set modal context
                    this.createInstanceModalContext.name = this._selectedServiceId;
                    this.createInstanceModalContext.data['documentSelfLink'] = this._selectedServiceId;
                    this.createInstanceModalContext.data['body'] = '';

                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceGetLinksSubscription)) {
            this._baseServiceGetLinksSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceGetChildServiceListSubscription)) {
            this._baseServiceGetChildServiceListSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._activatedRouteParamsSubscription)) {
            this._activatedRouteParamsSubscription.unsubscribe();
        }
    }

    getServiceLinks(): string[] {
        return this._serviceLinks;
    }

    getChildServiceLinks(): string[] {
        return _.map(this._childServicesLinks, (childServiceLink: string) => {
            return StringUtil.parseDocumentLink(childServiceLink).id;
        });
    }

    getSelectedServiceId(): string {
        return this._selectedServiceId;
    }

    getSelectedServiceRouterId(id: string): string {
        return StringUtil.encodeToId(id);
    }

    getSelectedChildServiceId(): string {
        return this._selectedChildServiceId;
    }

    onCreateInstance(event: MouseEvent): void {
        var selectedServiceId: string = this.createInstanceModalContext.data['documentSelfLink'];
        var body: string = this.createInstanceModalContext.data['body'];

        if (!selectedServiceId || !body) {
            return;
        }

        this._baseService.post(selectedServiceId, body).subscribe(
            (document: ServiceDocument) => {
                var documentId: string = document.documentSelfLink ?
                    StringUtil.parseDocumentLink(document.documentSelfLink).id : '';
                this._notificationService.set([{
                    type: 'SUCCESS',
                    messages: [`Instance ${documentId} Created`]
                }]);

                // Reset body
                this.createInstanceModalContext.data['body'] = '';
            },
            (error) => {
                // TODO: Better error handling
                this._notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    private _getData(): void {
        // Only get _serviceLinks once
        if (_.isEmpty(this._serviceLinks)) {
            // Reset _childServicesLinks when the service itself changes
            this._childServicesLinks = [];

            this._baseServiceGetLinksSubscription =
                this._baseService.post(URL.Root, URL.RootPostBody).subscribe(
                    (document: ServiceDocumentQueryResult) => {
                        this._serviceLinks = document.documentLinks;
                    },
                    (error) => {
                        // TODO: Better error handling
                        this._notificationService.set([{
                            type: 'ERROR',
                            messages: [`Failed to retrieve factory services: [${error.statusCode}] ${error.message}`]
                        }]);
                    });
        }

        // - When _childServicesLinks is not available, get it
        // - When switching between instances (thus _selectedChildServiceId is
        //      available), skip querying child services since it will
        //      not change anyway
        if (_.isEmpty(this._childServicesLinks) || _.isNull(this._selectedChildServiceId)) {
            this._baseServiceGetChildServiceListSubscription =
                this._baseService.getDocumentLinks(this._selectedServiceId).subscribe(
                    (childServiceLinks: string[]) => {
                        this._childServicesLinks = childServiceLinks;
                    },
                    (error) => {
                        // TODO: Better error handling
                        this._notificationService.set([{
                            type: 'ERROR',
                            messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                        }]);
                    });
        }
    }
}
