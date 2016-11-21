import { ServiceDocument } from '../index';

export interface SerializedOperation extends ServiceDocument {
    action: string;
    host: string;
    port: number;
    path: string;
    id: number;
    referer: string;
    jsonBody: string;
    statusCode: number;
    options: string[];

    query?: string;
    contextId?: string;
    transactionId?: string;
    userInfo?: string;
}
