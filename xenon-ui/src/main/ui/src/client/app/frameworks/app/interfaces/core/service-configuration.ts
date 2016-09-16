import { ServiceDocument } from '../index';

export interface ServiceConfiguration extends ServiceDocument {
    maintenanceIntervalMicros: number;
    operationQueueLimit: number;
    epoch: number;
    options: string[];
}
