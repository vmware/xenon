import * as _ from 'lodash';

import { Node, NodeGroupCircle } from '../../interfaces/index';

const EPSILON: number = 1e-12;

export class NodeGroupUtil {
    /*
     * For each the given node group, returns the smallest circle that enclose all the nodes inside
     * the group. Note that it's possible that one node belongs to multiple groups. In this case
     * multiple circles will enclose this particular node.
     *
     * Derived From: https://www.nayuki.io/res/smallest-enclosing-circle/smallestenclosingcircle.js
     */
    static getGroupCircles(nodesByGroup: {[key: string]: Node[]}): NodeGroupCircle[] {
        var groupCircles: NodeGroupCircle[] = [];

        _.each(nodesByGroup, (nodes: Node[], groupName: string) => {
            // Clone list to preserve the original data, do Fisher-Yates shuffle
            var shuffled: Node[] = _.shuffle(_.cloneDeep(nodes));

            // Progressively add nodes to circle or recompute circle
            var circle: NodeGroupCircle = null;
            for (let i: number = 0; i < shuffled.length; i++) {
                let node: Node = shuffled[i];
                if (circle === null || !this.isInCircle(circle, node)) {
                    circle = this.makeCircleOneNode(_.slice(shuffled, 0, i + 1), node);
                }
            }

            if (circle === null) {
                return;
            }

            groupCircles.push({
                group: groupName,
                x: circle.x,
                y: circle.y,
                r: circle.r
            });
        });

        return groupCircles;
    }

    /**
     * Whether a node belongs to the default node group
     */
    static belongsToDefaultGroup(groupReference: string): boolean {
        return /\/core\/node-groups\/default/.test(groupReference);
    }

    /**
     * One boundary nodes known
     */
    private static makeCircleOneNode(nodes: Node[], node0: Node): NodeGroupCircle {
        var circle: NodeGroupCircle = {
            x: node0.x,
            y: node0.y,
            r: 0
        };

        for (let i: number = 0; i < nodes.length; i++) {
            let node1: Node = nodes[i];
            if (!this.isInCircle(circle, node1)) {
                if (circle.r === 0) {
                    circle = this.makeDiameter(node0, node1);
                } else {
                    circle = this.makeCircleTwoNodes(nodes.slice(0, i + 1), node0, node1);
                }
            }
        }

        return circle;
    }

    /**
     * Two boundary nodes known
     */
    private static makeCircleTwoNodes(nodes: Node[], node0: Node, node1: Node): NodeGroupCircle {
        var temp: NodeGroupCircle = this.makeDiameter(node0, node1);
        var containsAll: boolean = true;

        for (let i: number = 0; i < nodes.length; i++) {
            containsAll = containsAll && this.isInCircle(temp, nodes[i]);
        }

        if (containsAll) {
            return temp;
        }

        var left: NodeGroupCircle = null;
        var right: NodeGroupCircle = null;

        for (let i: number = 0; i < nodes.length; i++) {
            let node2: Node = nodes[i];
            let cross: number = this.crossProduct(node0.x, node0.y, node1.x, node1.y, node2.x, node2.y);
            let circle: NodeGroupCircle = this.makeCircumCircle(node0, node1, node2);

            if (circle === null) {
                continue;
            } else if (cross > 0 && (left === null ||
                this.crossProduct(node0.x, node0.y, node1.x, node1.y, circle.x, circle.y) >
                this.crossProduct(node0.x, node0.y, node1.x, node1.y, left.x, left.y))) {
                left = circle;
            } else if (cross < 0 && (right === null ||
                this.crossProduct(node0.x, node0.y, node1.x, node1.y, circle.x, circle.y) <
                this.crossProduct(node0.x, node0.y, node1.x, node1.y, right.x, right.y))) {
                right = circle;
            }
        }

        return right === null || left !== null && left.r <= right.r ? left : right;
    }

    private static makeCircumCircle(node0: Node, node1: Node, node2: Node): NodeGroupCircle {
        // Mathematical algorithm from Wikipedia: Circumscribed circle
        var x0: number = node0.x;
        var y0: number = node0.y;
        var x1: number = node1.x;
        var y1: number = node1.y;
        var x2: number = node2.x;
        var y2: number = node2.y;

        var diameter: number = (x0 * (y1 - y2) + x1 * (y2 - y0) + x2 * (y0 - y1)) * 2;
        if (diameter === 0) {
            return null;
        }

        var x: number = ((x0 * x0 + y0 * y0) * (y1 - y2)
            + (x1 * x1 + y1 * y1) * (y2 - y0) + (x2 * x2 + y2 * y2) * (y0 - y1)) / diameter;
        var y: number = ((x0 * x0 + y0 * y0) * (x2 - x1)
            + (x1 * x1 + y1 * y1) * (x0 - x2) + (x2 * x2 + y2 * y2) * (x1 - x0)) / diameter;
        return {
            x: x,
            y: y,
            r: this.distance(x, y, x0, y0)
        };
    }

    private static makeDiameter(node0: Node, node1: Node): NodeGroupCircle {
        return {
            x: (node0.x + node1.x) / 2,
            y: (node0.y + node1.y) / 2,
            r: this.distance(node0.x, node0.y, node1.x, node1.y) / 2
        };
    }

    private static isInCircle(circle: NodeGroupCircle, node: Node): boolean {
        return circle !== null &&
            this.distance(node.x, node.y, circle.x, circle.y) < circle.r + EPSILON;
    }

    /**
     * Returns twice the signed area of the triangle defined by (x0, y0), (x1, y1), (x2, y2)
     */
    private static crossProduct(x0: number, y0: number,
            x1: number, y1: number, x2: number, y2: number): number {
        return (x1 - x0) * (y2 - y0) - (y1 - y0) * (x2 - x0);
    }

    private static distance(x0: number, y0: number, x1: number, y1: number): number {
        return Math.sqrt((x0 - x1) * (x0 - x1) + (y0 - y1) * (y0 - y1));
    }
}
