import { ServiceDocument } from '../index';

export interface ProcessLog extends ServiceDocument {
    items: string[];
}
