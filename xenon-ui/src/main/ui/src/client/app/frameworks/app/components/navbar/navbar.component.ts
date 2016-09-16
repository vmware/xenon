// angular
import { ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';

// app
import { BaseComponent } from '../../../core/index';

import { Node } from '../../interfaces/index';
import { AuthenticationService, NodeSelectorService, NotificationService } from '../../services/index';
import { ProcessingStageUtil } from '../../utils/index';

@BaseComponent({
    moduleId: module.id,
    selector: 'xe-navbar',
    templateUrl: 'navbar.component.html',
    styleUrls: ['navbar.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})
export class NavbarComponent implements OnInit, OnDestroy {
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
     * Flag indicating if the node selector component is active.
     */
    private _isNodeSelectorActive: boolean = false;

    /**
     * Subscriptions to services.
     */
    private _nodeSelectorServiceGetHostSubscription: Subscription;
    private _nodeSelectorServiceGetSelectedSubscription: Subscription;
    private _nodeSelectorServiceGetActiveSubscription: Subscription;

    constructor(
        private _authenticationService: AuthenticationService,
        private _nodeSelectorService: NodeSelectorService,
        private _notificationService: NotificationService,
        private _router: Router
    ) {}

    ngOnInit(): void {
        this._nodeSelectorServiceGetHostSubscription =
            this._nodeSelectorService.getHostNode().subscribe(
                (node: Node) => {
                    this._hostNode = node;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve host node: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this._nodeSelectorServiceGetSelectedSubscription =
            this._nodeSelectorService.getSelectedNode().subscribe(
                (node: Node) => {
                    this._selectedNode = node;
                },
                (error) => {
                    // TODO: Better error handling
                    this._notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve selected node: [${error.statusCode}] ${error.message}`]
                    }]);
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
        if (!_.isUndefined(this._nodeSelectorServiceGetActiveSubscription)) {
            this._nodeSelectorServiceGetActiveSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetSelectedSubscription)) {
            this._nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }

        if (!_.isUndefined(this._nodeSelectorServiceGetHostSubscription)) {
            this._nodeSelectorServiceGetHostSubscription.unsubscribe();
        }
    }

    getSelectedNodeId(): string {
        return this._selectedNode ? this._selectedNode.id : 'Unknown';
    }

    getSelectedNodeStatusClass(baseClass: string): string {
        var statusClass: string = this._selectedNode ?
            ProcessingStageUtil[this._selectedNode.status.toUpperCase()].className :
            ProcessingStageUtil['UNKNOWN'].className;

        return `${baseClass} ${statusClass}`;
    }

    isHostNode(): boolean {
        if (_.isUndefined(this._hostNode) || _.isUndefined(this._selectedNode)) {
            return false;
        }

        return this._hostNode.id === this._selectedNode.id;
    }

    isNodeSelectorActive(): boolean {
        return this._isNodeSelectorActive;
    }

    onNodeSelectorClicked($event: MouseEvent) {
        this._isNodeSelectorActive = !this._isNodeSelectorActive;
        this._nodeSelectorService.setNodeSelectorActive(this._isNodeSelectorActive);
    }

    onLogout(event: MouseEvent): void {
        this._authenticationService.logout();

        this._router.navigate(['/login']);
    }
}
