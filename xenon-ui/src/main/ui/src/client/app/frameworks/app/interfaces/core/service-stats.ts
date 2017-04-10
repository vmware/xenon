import { ServiceDocument, ServiceStatsEntry } from '../index';

export interface ServiceStats extends ServiceDocument {
    kind: string;
    entries: { [key: string]: ServiceStatsEntry | any };
}
