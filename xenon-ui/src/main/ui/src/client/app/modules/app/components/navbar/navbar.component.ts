// angular
import { AfterViewInit, Component, OnInit, OnDestroy } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs/Subscription';
import * as _ from 'lodash';

// app
import { Node } from '../../interfaces/index';
import { AuthenticationService, NodeSelectorService, NotificationService } from '../../services/index';
import { ProcessingStageUtil } from '../../utils/index';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: 'xe-navbar',
    templateUrl: 'navbar.component.html',
    styleUrls: ['navbar.component.css']
})
export class NavbarComponent implements OnInit, AfterViewInit, OnDestroy {/**
    /**
     * Flag indicating if the node selector component is active.
     */
    isNodeSelectorActive: boolean = false;

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
     * Subscriptions to services.
     */
    private nodeSelectorServiceGetHostSubscription: Subscription;
    private nodeSelectorServiceGetSelectedSubscription: Subscription;
    private nodeSelectorServiceGetActiveSubscription: Subscription;

    constructor(
        private authenticationService: AuthenticationService,
        private nodeSelectorService: NodeSelectorService,
        private notificationService: NotificationService,
        private router: Router
    ) {}

    ngOnInit(): void {
        this.nodeSelectorServiceGetHostSubscription =
            this.nodeSelectorService.getHostNode().subscribe(
                (node: Node) => {
                    this.hostNode = node;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve host node: [${error.statusCode}] ${error.message}`]
                    }]);
                });

        this.nodeSelectorServiceGetSelectedSubscription =
            this.nodeSelectorService.getSelectedNode().subscribe(
                (node: Node) => {
                    this.selectedNode = node;
                },
                (error) => {
                    // TODO: Better error handling
                    this.notificationService.set([{
                        type: 'ERROR',
                        messages: [`Failed to retrieve selected node: [${error.statusCode}] ${error.message}`]
                    }]);
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

    ngAfterViewInit(): void {
        jQuery('.nav-link').tooltip({
            placement: 'bottom',
            trigger: 'hover'
        });
    }

    ngOnDestroy(): void {
        // Workaround to dispose the tooltip
        var tooltips = jQuery('.tooltip');
        if (tooltips) {
            tooltips.remove();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetActiveSubscription)) {
            this.nodeSelectorServiceGetActiveSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetSelectedSubscription)) {
            this.nodeSelectorServiceGetSelectedSubscription.unsubscribe();
        }

        if (!_.isUndefined(this.nodeSelectorServiceGetHostSubscription)) {
            this.nodeSelectorServiceGetHostSubscription.unsubscribe();
        }
    }

    getSelectedNodeId(): string {
        return this.selectedNode ? this.selectedNode.id : 'Unknown';
    }

    getSelectedNodeStatusClass(baseClass: string): string {
        var statusClass: string = this.selectedNode ?
            ProcessingStageUtil[this.selectedNode.status.toUpperCase()].className :
            ProcessingStageUtil['UNKNOWN'].className;

        return `${baseClass} ${statusClass}`;
    }

    isHostNode(): boolean {
        if (_.isUndefined(this.hostNode) || _.isUndefined(this.selectedNode)) {
            return false;
        }

        return this.hostNode.id === this.selectedNode.id;
    }

    onNodeSelectorClicked($event: MouseEvent) {
        this.isNodeSelectorActive = !this.isNodeSelectorActive;
        this.nodeSelectorService.setNodeSelectorActive(this.isNodeSelectorActive);
    }

    onLogout(event: MouseEvent): void {
        this.authenticationService.logout().subscribe(
            (data) => {
                this.router.navigate(['/login']);
            },
            (error) => {
                // TODO: Better error handling
                this.notificationService.set([{
                    type: 'ERROR',
                    messages: [`Failed to log out: [${error.statusCode}] ${error.message}`]
                }]);
            });
    }
}
