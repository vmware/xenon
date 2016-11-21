// angular
import { AfterViewInit, ChangeDetectionStrategy, EventEmitter, Input,
    OnChanges, OnDestroy, Output, SimpleChange } from '@angular/core';
import * as _ from 'lodash';

// app
import { BaseComponent } from '../../../core/index';

import { NodeGroupUtil } from './node-group.util';

import { Link, NodeGroup, NodeGroupCircle, Node } from '../../interfaces/index';
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
     * The d3 group configurations
     */
    private _groupConfig: any;

    /**
     * The d3 tooltip for nodes
     */
    private _nodeTooltip: any;

    /**
     * The d3 tooltip for groups
     */
    private _groupTooltip: any;

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

        this._nodeTooltip = d3.tip()
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

                return `Node: <strong>${node.id}</strong><small>${stateSuffix}</small><br>
                        Status: <strong>${node.status}</strong><br>
                        Membership Quorum: <strong>${node.membershipQuorum}</strong><br>
                        Options: <strong>${node.options.join(' ')}</strong><br>`;
            });

        this._groupTooltip = d3.tip()
            .attr('class', 'chart-tooltip')
            .offset([-9, 0])
            .html((group: NodeGroupCircle) => {
                return `Group: <strong>${group.group}</strong>`;
            });

        this._canvas = d3.select('svg').call(this._nodeTooltip).call(this._groupTooltip);

        this._forceLayout = d3.layout.force()
            .charge(-2000)
            .chargeDistance(2000)
            .linkDistance(100)
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

    private _getNodesByName(): {[key: string]: Node} {
        var nodesByName: {[key: string]: Node} = {};

        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return nodesByName;
        }

        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            _.each(nodeGroup.nodes, (node: Node) => {
                // Keep the existing node if it belongs to the default group
                if (nodesByName.hasOwnProperty(node.id) &&
                    NodeGroupUtil.belongsToDefaultGroup(nodesByName[node.id].groupReference)) {
                    return;
                }

                nodesByName[node.id] = node;
            });
        });

        return nodesByName;
    }

    private _getLinks(nodesByName: {[key: string]: Node}): Link[] {
        var links: Link[] = [];

        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return links;
        }

        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            _.each(nodeGroup.nodes, (sourceNode: Node) => {
                _.each(nodeGroup.nodes, (targetNode: Node) => {
                    if (sourceNode.id === targetNode.id) {
                        return;
                    }

                    links.push({
                        source: nodesByName[sourceNode.id],
                        target: nodesByName[targetNode.id]
                    });
                });
            });
        });

        return links;
    }

    private _getNodesByGroup(nodesByName: {[key: string]: Node}): {[key: string]: Node[]} {
        var nodesByGroup: {[key: string]: Node[]} = {};

        if (_.isUndefined(this.nodeGroups) || _.isEmpty(this.nodeGroups)) {
            return nodesByGroup;
        }

        _.each(this.nodeGroups, (nodeGroup: NodeGroup) => {
            var nodes: Node[] = [];

            _.each(nodeGroup.nodes, (node: Node) => {
                // In case a node belongs to multiple group, the same instance
                // will be pushed to each group.
                var uniqNode: Node = nodesByName[node.id];
                nodes.push(uniqNode);
            });

            nodesByGroup[nodeGroup.documentSelfLink] = nodes;
        });

        return nodesByGroup;
    }

    private _render(): void {
        // Clears the link and nodes to prepare for refresh
        if (!_.isUndefined(this._groupConfig)) {
            this._groupConfig.remove();
        }

        if (!_.isUndefined(this._linkConfig)) {
            this._linkConfig.remove();
        }

        if (!_.isUndefined(this._nodeConfig)) {
            this._nodeConfig.remove();
        }

        var nodesByName: {[key: string]: Node} = this._getNodesByName();
        var nodesByGroup: {[key: string]: Node[]} = this._getNodesByGroup(nodesByName);
        var links: Link[] = this._getLinks(nodesByName);
        var nodes: Node[] = _.values(nodesByName);

        this._forceLayout
            .nodes(nodes)
            .links(links)
            .start();

        this._groupConfig = this._canvas.selectAll('.group')
                .data(NodeGroupUtil.getGroupCircles(nodesByGroup))
            .enter()
                .append('g')
                    .append('circle')
                        .attr('class', (nodeGroup: NodeGroupCircle) => {
                            return this._getNodeGroupClass(nodeGroup.group);
                        })
                        .on('mouseover', this._groupTooltip.show)
                        .on('mouseout', this._groupTooltip.hide);

        this._linkConfig = this._canvas.selectAll('.link')
                .data(links)
            .enter().append('line')
                .attr('class', 'link');

        this._nodeConfig = this._canvas.selectAll('.node')
                .data(nodes)
            .enter()
                .append('g')
                    .append('circle')
                        .attr('r', '16px')
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
                        })
                        .on('mouseover', this._nodeTooltip.show)
                        .on('mouseout', this._nodeTooltip.hide);

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

                this._groupConfig.data(NodeGroupUtil.getGroupCircles(nodesByGroup))
                    .attr('transform', (d: any) => {
                        return 'translate(' + d.x + ',' + d.y + ')';
                    })
                    .attr('r', (d: any) => {
                        return d.r + 32;
                    });
            });
    }

    private _getNodeGroupClass(group: string): string {
        // 7 different colors are available, if the group number go beyond 7,
        // start from 0
        var index: number = this.nodeGroups ? _.findIndex(this.nodeGroups, (nodeGroup: NodeGroup) => {
            return nodeGroup.documentSelfLink === group;
        }) % 7 : 0;
        return `group group-${index}`;
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
