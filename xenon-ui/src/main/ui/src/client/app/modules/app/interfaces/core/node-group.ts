import { Node, ServiceDocument } from '../index';

export interface NodeGroup extends ServiceDocument {
    config: {[key: string]: any};
    nodes: {[key: string]: Node};
    membershipUpdateTimeMicros: number;
}
