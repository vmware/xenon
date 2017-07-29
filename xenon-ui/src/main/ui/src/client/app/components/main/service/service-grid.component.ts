// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../../modules/app/enums/index';
import { EventContext, ModalContext, Node, ServiceDocument, ServiceDocumentQueryResult } from '../../../modules/app/interfaces/index';
import { ODataUtil, StringUtil } from '../../../modules/app/utils/index';
import { BaseService, NodeSelectorService, NotificationService } from '../../../modules/app/services/index';

@Component({
    selector: 'xe-service-grid',
    moduleId: module.id,
    templateUrl: './service-grid.component.html',
    styleUrls: ['./service-grid.component.css']
})

export class ServiceGridComponent implements OnInit, OnDestroy {
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
     * The core factory services in the view.
     */
    private coreServices: ServiceDocument[] = [];

    /**
     * The custom factory services in the view.
     */
    private customServices: ServiceDocument[] = [];

    /**
     * The regexp for checking if one is a core service based on
     * the service link.
     */
    private coreServiceRegExp: RegExp = /\/core\/[\S]*/i;

    /**
     * Subscriptions to services.
     */
    private baseServiceGetLinksSubscription: Subscription;
    private baseServiceGetDocumentSubscription: Subscription;
    private nodeSelectorServiceGetSelectedSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private nodeSelectorService: NodeSelectorService,
        private notificationService: NotificationService) {}

    ngOnInit(): void {
        this._getData();

        // Update data when selected node changes
        this.nodeSelectorServiceGetSelectedSubscription =
            this.nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceGetLinksSubscription)) {
            this.baseServiceGetLinksSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetDocumentSubscription)) {
            this.baseServiceGetDocumentSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetSelectedSubscription)) {
            this.nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    getCoreServices(): ServiceDocument[] {
        return this.coreServices;
    }

    getCustomServices(): ServiceDocument[] {
        return this.customServices;
    }

    getServiceId(documentSelfLink: string): string {
        return documentSelfLink ? StringUtil.encodeToId(documentSelfLink) : '';
    }

    onCreateChildServiceClicked(context: EventContext): void {
        this.createChildServiceModalContext.name = context.data['documentSelfLink'];
        this.createChildServiceModalContext.data['documentSelfLink'] = context.data['documentSelfLink'];
        this.createChildServiceModalContext.data['body'] = '';
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
            },
            (error) => {
                // TODO: Better error handling
                this.notificationService.set([{
                    type: 'ERROR',
                    messages: [`[${error.statusCode}] ${error.message}`]
                }]);
            });
    }

    private _getData(): void {
        this.baseServiceGetLinksSubscription =
            this.baseService.post(URL.Root, URL.RootPostBody).subscribe(
                (document: ServiceDocumentQueryResult) => {
                    this.baseServiceGetDocumentSubscription =
                        this.baseService.getDocuments(document.documentLinks, `${ODataUtil.count()}`).subscribe(
                            (services: ServiceDocument[]) => {
                                this.coreServices = _.sortBy(_.filter(services, (service: ServiceDocument) => {
                                    return this.coreServiceRegExp.test(service.documentSelfLink);
                                }), 'documentSelfLink');

                                this.customServices = _.sortBy(_.filter(services, (service: ServiceDocument) => {
                                    return !this.coreServiceRegExp.test(service.documentSelfLink);
                                }), 'documentSelfLink');
                            },
                            (error) => {
                                // TODO: Better error handling
                                this.notificationService.set([{
                                    type: 'ERROR',
                                    messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                                }]);
                            });
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
