// angular
import { Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router, UrlTree } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { URL } from '../../enums/index';
import { NodeGroup, Node, ServiceDocumentQueryResult } from '../../interfaces/index';
import { BaseService, NodeSelectorService,
    NotificationService } from '../../services/index';

@Component({
    selector: 'xe-node-selector',
    moduleId: module.id,
    templateUrl: './node-selector.component.html',
    styleUrls: ['./node-selector.component.css']
})

export class NodeSelectorComponent implements OnInit, OnDestroy {
    /**
     * The node group data retrieved from server.
     */
    private nodeGroups: NodeGroup[];

    /**
     * The id for the selected node.
     */
    private selectedNodeId: string = '';

    /**
     * The node that hosts the current application.
     */
    private hostNode: Node;

    /**
     * The node whose information is being displayed in the views (not the node selector).
     * It starts with the host node, and can be changes by user.
     */
    private selectedNode: Node;

    /**
     * The node that is temporary being highlighted in the node selector. Until user confirms,
     * the node will not become selected node.
     */
    private highlightedNode: Node;

    /**
     * Flag indicating if the node selector component is active.
     */
    private isNodeSelectorActive: boolean = false;

    /**
     * Subscriptions to services.
     */
    private activatedRouteSubscription: Subscription;
    private baseServiceGetDocumentSubscription: Subscription;
    private baseServiceGetDocumentsSubscription: Subscription;
    private nodeSelectorServiceGetActiveSubscription: Subscription;

    constructor(
        private baseService: BaseService,
        private nodeSelectorService: NodeSelectorService,
        private notificationService: NotificationService,
        private activatedRoute: ActivatedRoute,
        private router: Router) {}

    ngOnInit(): void {
        this.activatedRouteSubscription =
            this.activatedRoute.queryParams.subscribe(
                (params: {[key: string]: any}) => {
                    this.selectedNodeId = params['node'];

                    if (!_.isUndefined(this.nodeGroups) && !_.isEmpty(this.nodeGroups)) {
                        if (this.selectedNodeId && this.selectedNodeId !== 'undefined') {
                            this.selectedNode = this.getSelectedNodeById(this.selectedNodeId);
                        } else {
                            this.selectedNode = this.hostNode;
                        }

                        this.nodeSelectorService.setSelectedNode(this.selectedNode);
                        return;
                    }

                    // Only request once on init
                    this.getData();
                });

        this.nodeSelectorServiceGetActiveSubscription =
            this.nodeSelectorService.getNodeSelectorActive().subscribe(
                (isActive: boolean) => {
                    this.isNodeSelectorActive = isActive;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve node selector states: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this.baseServiceGetDocumentsSubscription)) {
            this.baseServiceGetDocumentsSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.baseServiceGetDocumentSubscription)) {
            this.baseServiceGetDocumentSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetActiveSubscription)) {
            this.nodeSelectorServiceGetActiveSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.activatedRouteSubscription)) {
            this.activatedRouteSubscription.unsubscribe();
        }
    }

    getNodeGroups(): NodeGroup[] {
        return this.nodeGroups ? this.nodeGroups : [];
    }

    getHostNode(): Node {
        return this.hostNode;
    }

    getSelectedNode(): Node {
        return this.selectedNode;
    }

    getHighlightedNode(): Node {
        return this.highlightedNode;
    }

    isViewVisible(): boolean {
        return this.isNodeSelectorActive;
    }

    isHostNode(): boolean {
        if (_.isUndefined(this.hostNode) || _.isUndefined(this.selectedNode)) {
            return false;
        }

        return this.hostNode.id === this.selectedNode.id;
    }

    onNodeHighlighted(node: Node): void {
        this.highlightedNode = node;
    }

    onNodeSelectionConfirmed(nodeId: string): void {
        this.selectedNode = this.getSelectedNodeById(nodeId);
        this.nodeSelectorService.setSelectedNode(this.selectedNode);

        this.isNodeSelectorActive = false;
        this.nodeSelectorService.setNodeSelectorActive(this.isNodeSelectorActive);

        // TODO: Figure out better ways to do this
        var urlTree: UrlTree =
            this.router.createUrlTree(['./'], {
                relativeTo: this.activatedRoute.firstChild,
                queryParams: {'node': nodeId},
                skipLocationChange: true
            });
        this.router.navigateByUrl(urlTree);
    }

    onViewClosed(event: Event): void {
        this.isNodeSelectorActive = false;
        this.nodeSelectorService.setNodeSelectorActive(this.isNodeSelectorActive);
    }

    private getSelectedNodeById(nodeId: string): Node {
        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return null;
        }

        var node: Node;
        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            if (nodeGroup.nodes.hasOwnProperty(nodeId)) {
                node = nodeGroup.nodes[nodeId];
            }
        });

        return node;
    }

    private getData(): void {
        this.baseServiceGetDocumentSubscription =
            this.baseService.getDocument(URL.NodeGroup, '', false).subscribe(
                (document: ServiceDocumentQueryResult) => {
                    // If no node is provided through query parameter,
                    // use the current node's documentOwner property as the selectedNodeId
                    this.selectedNodeId = this.selectedNodeId && this.selectedNodeId !== 'undefined' ?
                        this.selectedNodeId : document.documentOwner;

                    this.baseServiceGetDocumentsSubscription =
                        this.baseService.getDocuments(document.documentLinks, '', false).subscribe(
                            (nodeGroups: NodeGroup[]) => {
                                this.nodeGroups = nodeGroups;
                                this.hostNode = this.getSelectedNodeById(document.documentOwner);
                                this.selectedNode = this.getSelectedNodeById(this.selectedNodeId);
                                // Highlight the selected node to begin with
                                this.highlightedNode = this.selectedNode;

                                this.nodeSelectorService.setHostNode(this.hostNode);
                                this.nodeSelectorService.setSelectedNode(this.selectedNode);
                            },
                            (error) => {
                                // TODO: Better error handling
                                this.notificationService.set([{
                                    type: 'ERROR',
                                    messages: [`Failed to retrieve node group details: [${error.statusCode}] ${error.message}`]
                                }]);
                            });
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve node group: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
