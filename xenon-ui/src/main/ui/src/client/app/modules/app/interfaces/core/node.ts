import { ServiceDocument } from '../index';

export interface Node extends ServiceDocument {
    groupReference: string;
    status: string;
    options: string[];
    id: string;
    membershipQuorum: number;

    // Added by D3 when renderring
    x?: number;
    y?: number;
}
