import { Node } from '../index';

/**
 * Link is a D3 specific element, which represents a connection
 * between the nodes.
 */
export interface Link {
    source: Node;
    target: Node;
}
