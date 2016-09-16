// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../frameworks/core/index';

import { ServiceCardComponent } from './service-card.component';

import { URL } from '../../../frameworks/app/enums/index';
import { EventContext, ModalContext, Node, ServiceDocument } from '../../../frameworks/app/interfaces/index';
import { StringUtil } from '../../../frameworks/app/utils/index';

import { BaseService, NodeSelectorService, NotificationService } from '../../../frameworks/app/services/index';

@BaseComponent({
    selector: 'xe-service-grid',
    moduleId: module.id,
    templateUrl: './service-grid.component.html',
    styleUrls: ['./service-grid.component.css'],
    directives: [ServiceCardComponent],
    changeDetection: ChangeDetectionStrategy.Default
})

export class ServiceGridComponent implements OnInit, OnDestroy {
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
     * The core factory services in the view.
     */
    private _coreServices: ServiceDocument[];

    /**
     * The custom factory services in the view.
     */
    private _customServices: ServiceDocument[];

    /**
     * The regexp for checking if one is a core service based on
     * the service link.
     */
    private _coreServiceRegExp: RegExp = /\/core\/[\S]*/i;

    /**
     * Subscriptions to services.
     */
    private _baseServiceGetLinksSubscription: Subscription;
    private _baseServiceGetDocumentSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService,
        private _router: Router) {}

    ngOnInit(): void {
        this._getData();

        // Update data when selected node changes
        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (selectedNode: Node) => {
                    this._getData();
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceGetLinksSubscription)) {
            this._baseServiceGetLinksSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceGetDocumentSubscription)) {
            this._baseServiceGetDocumentSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }
    }

    getCoreServices(): ServiceDocument[] {
        return this._coreServices ? this._coreServices : [];
    }

    getCustomServices(): ServiceDocument[] {
        return this._customServices ? this._customServices : [];
    }

    getServiceId(documentSelfLink: string): string {
        return documentSelfLink ? StringUtil.encodeToId(documentSelfLink) : '';
    }

    onCreateInstanceClicked(context: EventContext): void {
        this.createInstanceModalContext.name = context.data['documentSelfLink'];
        this.createInstanceModalContext.data['documentSelfLink'] = context.data['documentSelfLink'];
        this.createInstanceModalContext.data['body'] = null;
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
        this._baseServiceGetLinksSubscription =
            this._baseService.getDocumentLinks(URL.Root).subscribe(
                (links: string[]) => {
                    this._baseServiceGetDocumentSubscription =
                        this._baseService.getDocuments(links).subscribe(
                            (services: ServiceDocument[]) => {
                                this._coreServices = _.filter(services, (service: ServiceDocument) => {
                                    return this._coreServiceRegExp.test(service.documentSelfLink);
                                });

                                this._customServices = _.filter(services, (service: ServiceDocument) => {
                                    return !this._coreServiceRegExp.test(service.documentSelfLink);
                                });
                            },
                            (error) => {
                                // TODO: Better error handling
                                this._notificationService.set([{
                                    type: 'ERROR',
                                    messages: [`Failed to retrieve factory service details: [${error.statusCode}] ${error.message}`]
                                }]);
                            });
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
