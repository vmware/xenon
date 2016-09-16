// angular
import { AfterViewInit, ChangeDetectionStrategy, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';

import { BaseComponent } from '../../../core/index';

import { Link, NodeGroup, Node } from '../../interfaces/index';
import { ProcessingStageUtil } from '../../utils/index';

declare var d3: any;

@BaseComponent({
    selector: 'xe-node-canvas',
    moduleId: module.id,
    templateUrl: './node-canvas.component.html',
    styleUrls: ['./node-canvas.component.css'],
    changeDetection: ChangeDetectionStrategy.Default
})

/**
 * A standlone component that renders node topology
 */
export class NodeCanvasComponent implements AfterViewInit, OnChanges, OnDestroy {
    /**
     * The node groups with all the nodes to be rendered.
     */
    @Input()
    nodeGroups: NodeGroup[];

    /**
     * The id of the node that hosts the current application.
     */
    @Input()
    hostNodeId: string;

    /**
     * The id of the node whose information is being displayed in the views (not the node selector).
     */
    @Input()
    selectedNodeId: string;

    /**
     * The id of the node that is temporary being highlighted in the node selector.
     * Until user confirms, the node will not become selected node.
     */
    @Input()
    highlightedNodeId: string;

    /**
     * Emit the event with the node when user highlights a node on the graph.
     */
    @Output()
    highlightNode = new EventEmitter<Node>();

    /**
     * The canvas to be rendered on
     */
    private _canvas: any;

    /**
     * The d3 force layout
     */
    private _forceLayout: any;

    /**
     * The d3 node configurations
     */
    private _nodeConfig: any;

    /**
     * The d3 link configurations
     */
    private _linkConfig: any;

    /**
     * The d3 tooltip
     */
    private _tooltip: any;

    ngOnChanges(changes: {[propertyName: string]: SimpleChange}): void {
        var selectedNodeIdChanges = changes['selectedNodeId'];

        if (!selectedNodeIdChanges
            || _.isEmpty(selectedNodeIdChanges.currentValue)) {
            return;
        }

        // Clears the link and nodes to prepare for refresh
        this.selectedNodeId = selectedNodeIdChanges.currentValue;

        this._render();
    }

    /**
     * Initiate the basic DOM related variables after DOM is fully rendered.
     */
    ngAfterViewInit(): void {
        var canvasDom = document.querySelector('svg');
        var canvasWidth = canvasDom.parentElement.offsetWidth;
        var canvasHeight = canvasDom.parentElement.offsetHeight;

        this._tooltip = d3.tip()
            .attr('class', 'chart-tooltip')
            .offset([-12, 0])
            .html((node: Node) => {
                var states: string[] = [];

                if (this.hostNodeId && !_.isUndefined(node) && !_.isEmpty(node)
                        && this.hostNodeId === node.id) {
                    states.push('Host');
                }

                if (this.selectedNodeId && !_.isUndefined(node) && !_.isEmpty(node)
                        && this.selectedNodeId === node.id) {
                    states.push('Current');
                }

                var stateSuffix: string = states.length !==  0 ? ` (${states.join(', ')})` : '';

                return `<p>Node: <strong>${node.id}</strong><small>${stateSuffix}</small></p>
                        <p>Status: <strong>${node.status}</strong></p>
                        <p>Membership Quorum: <strong>${node.membershipQuorum}</strong></p>
                        <p class="m-b-0">Options: <strong>${node.options.join(' ')}</strong></p>`;
            });

        this._canvas = d3.select('svg').call(this._tooltip);

        this._forceLayout = d3.layout.force()
            .charge(-100)
            .linkDistance(120)
            .size([canvasWidth, canvasHeight]);
    }

    ngOnDestroy(): void {
        // Clear all the event handlers
        this._canvas.selectAll('circle')
            .on('click', null)
            .on('mouseover', null)
            .on('mouseout', null);

        this._forceLayout.on('tick', null);

        if (!_.isUndefined(this._linkConfig)) {
            this._linkConfig.remove();
        }

        if (!_.isUndefined(this._nodeConfig)) {
            this._nodeConfig.remove();
        }
    }

    private _getNodes(): Node[] {
        var nodes: Node[] = [];

        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return nodes;
        }

        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            _.each(nodeGroup.nodes, (node: Node) => {
                nodes.push(node);
            });
        });

        return nodes;
    }

    private _getLinks(): Link[] {
        var links: Link[] = [];

        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return links;
        }

        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            // Interconnect all the nodes within each group
            var nodeGroupSize: number = _.size(nodeGroup.nodes);

            for (let sourceId: number = 0; sourceId < nodeGroupSize; sourceId++) {
                for (let targetId: number = 0; targetId < nodeGroupSize; targetId++) {
                    if (sourceId === targetId) {
                        continue;
                    }

                    links.push({
                        source: sourceId,
                        target: targetId
                    });
                }
            }
        });

        return links;
    }

    private _render(): void {
        // Clears the link and nodes to prepare for refresh
        if (!_.isUndefined(this._linkConfig)) {
            this._linkConfig.remove();
        }

        if (!_.isUndefined(this._nodeConfig)) {
            this._nodeConfig.remove();
        }

        var nodes: Node[] = this._getNodes();
        var links: Link[] = this._getLinks();

        this._forceLayout
            .nodes(nodes)
            .links(links)
            .start();

        this._linkConfig = this._canvas.selectAll('.link')
                .data(links)
            .enter().append('line')
                .attr('class', 'link');

        this._nodeConfig = this._canvas.selectAll('.node')
                .data(nodes)
            .enter()
                .append('g')
                    .append('circle')
                        .attr('r', '1rem')
                        .attr('class', (node: Node) => {
                            return this._getNodeClassByStatus(node.status);
                        })
                        .classed('highlight', (node: Node) => {
                            if (!this.highlightedNodeId || _.isUndefined(node) || _.isEmpty(node)) {
                                return false;
                            }
                            return this.highlightedNodeId === node.id;
                        })
                        .classed('active', (node: Node) => {
                            if (!this.selectedNodeId || _.isUndefined(node) || _.isEmpty(node)) {
                                return false;
                            }
                            return this.selectedNodeId === node.id;
                        })
                        .classed('host', (node: Node) => {
                            if (!this.hostNodeId || _.isUndefined(node) || _.isEmpty(node)) {
                                return false;
                            }
                            return this.hostNodeId === node.id;
                        })
                        .on('click', (node: Node) => {
                            var target: any = d3.select((d3.event as Event).target);

                            var isNodeHighlighted = target.classed('highlight');

                            if (!isNodeHighlighted) {
                                this.highlightNode.emit(node);
                                this.highlightedNodeId = node.id;

                                this._canvas.selectAll('.node').classed('highlight', false);
                                target.classed('highlight', true);
                            }
                        }).
                        on('mouseover', this._tooltip.show).
                        on('mouseout', this._tooltip.hide);

        this._forceLayout.on('tick',
            () => {
                this._linkConfig
                    .attr('x1', (d: any) => { return d.source.x; })
                    .attr('y1', (d: any) => { return d.source.y; })
                    .attr('x2', (d: any) => { return d.target.x; })
                    .attr('y2', (d: any) => { return d.target.y; });

                this._nodeConfig.attr('transform', (d: any) => {
                        return 'translate(' + d.x + ',' + d.y + ')';
                    });
            });
    }

    private _getNodeClassByStatus(status: string): string {
        var statusClass: string = '';

        if (!status || !ProcessingStageUtil[status.toUpperCase()]) {
            statusClass = ProcessingStageUtil['UNKNOWN'].className;
        }

        statusClass = ProcessingStageUtil[status.toUpperCase()].className;
        return `node ${statusClass}`;
    }
}
