// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router, UrlTree } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../core/index';

import { URL } from '../../enums/index';
import { NodeGroup, Node, ServiceDocumentQueryResult } from '../../interfaces/index';
import { BaseService, NodeSelectorService,
    NotificationService } from '../../services/index';

import { NodeCanvasComponent } from './node-canvas.component';
import { NodeInfoPanelComponent } from './node-info-panel.component';

@BaseComponent({
    selector: 'xe-node-selector',
    moduleId: module.id,
    templateUrl: './node-selector.component.html',
    styleUrls: ['./node-selector.component.css'],
    directives: [NodeCanvasComponent, NodeInfoPanelComponent],
    changeDetection: ChangeDetectionStrategy.Default
})

export class NodeSelectorComponent implements OnInit, OnDestroy {
    /**
     * The node group data retrieved from server.
     */
    private _nodeGroups: NodeGroup[];

    /**
     * The id for the selected node.
     */
    private _selectedNodeId: string;

    /**
     * The node that hosts the current application.
     */
    private _hostNode: Node;

    /**
     * The node whose information is being displayed in the views (not the node selector).
     * It starts with the host node, and can be changes by user.
     */
    private _selectedNode: Node;

    /**
     * The node that is temporary being highlighted in the node selector. Until user confirms,
     * the node will not become selected node.
     */
    private _highlightedNode: Node;

    /**
     * Flag indicating if the node selector component is active.
     */
    private _isNodeSelectorActive: boolean = false;

    /**
     * Subscriptions to services.
     */
    private _activatedRouteSubscription: Subscription;
    private _baseServiceGetDocumentSubscription: Subscription;
    private _baseServiceGetDocumentsSubscription: Subscription;
    private _nodeSelectorServiceGetActiveSubscription: Subscription;

    constructor(
        private _baseService: BaseService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService,
        private _activatedRoute: ActivatedRoute,
        private _router: Router) {}

    ngOnInit(): void {
        this._activatedRouteSubscription =
            this._activatedRoute.queryParams.subscribe(
                (params: {[key: string]: any}) => {
                    this._selectedNodeId = params['node'];

                    if (!_.isUndefined(this._nodeGroups) && !_.isEmpty(this._nodeGroups)) {
                        if (this._selectedNodeId && this._selectedNodeId !== 'undefined') {
                            this._selectedNode = this._getSelectedNodeById(this._selectedNodeId);
                        } else {
                            this._selectedNode = this._hostNode;
                        }

                        this._nodeSelectorService.setSelectedNode(this._selectedNode);
                        return;
                    }

                    // Only request once on init
                    this._getData();
                });

        this._nodeSelectorServiceGetActiveSubscription =
            this._nodeSelectorService.getNodeSelectorActive().subscribe(
                (isActive: boolean) => {
                    this._isNodeSelectorActive = isActive;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve node selector states: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }

    ngOnDestroy(): void {
        if (!_.isUndefined(this._baseServiceGetDocumentsSubscription)) {
            this._baseServiceGetDocumentsSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._baseServiceGetDocumentSubscription)) {
            this._baseServiceGetDocumentSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetActiveSubscription)) {
            this._nodeSelectorServiceGetActiveSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._activatedRouteSubscription)) {
            this._activatedRouteSubscription.unsubscribe();
        }
    }

    getNodeGroups(): NodeGroup[] {
        return this._nodeGroups ? this._nodeGroups : [];
    }

    getHostNode(): Node {
        return this._hostNode;
    }

    getSelectedNode(): Node {
        return this._selectedNode;
    }

    getHighlightedNode(): Node {
        return this._highlightedNode;
    }

    isViewVisible(): boolean {
        return this._isNodeSelectorActive;
    }

    isHostNode(): boolean {
        if (_.isUndefined(this._hostNode) || _.isUndefined(this._selectedNode)) {
            return false;
        }

        return this._hostNode.id === this._selectedNode.id;
    }

    onNodeHighlighted(node: Node): void {
        this._highlightedNode = node;
    }

    onNodeSelectionConfirmed(nodeId: string): void {
        this._selectedNode = this._getSelectedNodeById(nodeId);
        this._nodeSelectorService.setSelectedNode(this._selectedNode);

        this._isNodeSelectorActive = false;
        this._nodeSelectorService.setNodeSelectorActive(this._isNodeSelectorActive);

        // TODO: Figure out better ways to do this
        var urlTree: UrlTree =
            this._router.createUrlTree(['./'], {
                relativeTo: this._activatedRoute.firstChild,
                queryParams: {'node': nodeId},
                skipLocationChange: true
            });
        this._router.navigateByUrl(urlTree);
    }

    onViewClosed(event: Event): void {
        this._isNodeSelectorActive = false;
        this._nodeSelectorService.setNodeSelectorActive(this._isNodeSelectorActive);
    }

    private _getSelectedNodeById(nodeId: string): Node {
        if (_.isUndefined(this._nodeGroups) || _.isEmpty(this._nodeGroups)) {
            return null;
        }

        var node: Node;
        _.each(this._nodeGroups, (nodeGroup: NodeGroup) => {
            if (nodeGroup.nodes.hasOwnProperty(nodeId)) {
                node = nodeGroup.nodes[nodeId];
            }
        });

        return node;
    }

    private _getData(): void {
        this._baseServiceGetDocumentSubscription =
            this._baseService.getDocument(URL.NodeGroup, false).subscribe(
                (document: ServiceDocumentQueryResult) => {
                    // If no node is provided through query parameter,
                    // use the current node's documentOwner property as the selectedNodeId
                    this._selectedNodeId = this._selectedNodeId && this._selectedNodeId !== 'undefined' ?
                        this._selectedNodeId : document.documentOwner;

                    this._baseServiceGetDocumentsSubscription =
                        this._baseService.getDocuments(document.documentLinks, false).subscribe(
                            (nodeGroups: NodeGroup[]) => {
                                this._nodeGroups = nodeGroups;
                                this._hostNode = this._getSelectedNodeById(document.documentOwner);
                                this._selectedNode = this._getSelectedNodeById(this._selectedNodeId);
                                // Highlight the selected node to begin with
                                this._highlightedNode = this._selectedNode;

                                this._nodeSelectorService.setHostNode(this._hostNode);
                                this._nodeSelectorService.setSelectedNode(this._selectedNode);
                            },
                            (error) => {
                                // TODO: Better error handling
                                this._notificationService.set([{
                                    type: 'ERROR',
                                    messages: [`Failed to retrieve node group details: [${error.statusCode}] ${error.message}`]
                                }]);
                            });
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve node group: [${error.statusCode}] ${error.message}`]
                    }]);
                });
    }
}
