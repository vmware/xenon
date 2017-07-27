import { ServiceDocument, SystemHostInfo } from '../index';

export interface ServiceHostState extends ServiceDocument {
    bindAddress: string;
    httpPort: number;
    httpsPort: number;
    maintenanceIntervalMicros: number;
    operationTimeoutMicros: number;
    serviceCacheClearDelayMicros: number;
    sslClientAuthMode: string;
    responsePayloadSizeLimit: number;
    requestPayloadSizeLimit: number;
    id: string;
    isPeerSynchronizationEnabled: boolean;
    peerSynchronizationTimeLimitSeconds: number;
    isAuthorizationEnabled: boolean;
    systemInfo: SystemHostInfo;
    lastMaintenanceTimeUtcMicros: number;
    isProcessOwner: boolean;
    isServiceStateCaching: boolean;
    codeProperties: {[key: string]: any};
    serviceCount: number;
    relativeMemoryLimits: {[key: string]: any};
    requestRateLimits: {[key: string]: any};
    initialPeerNodes: string[];

    publicUri?: string;

    storageSandboxFileReference?: string;
    resourceSandboxFileReference?: string;
    privateKeyFileReference?: string;
    privateKeyPassphrase?: string;
    certificateFileReference?: string;

    documentIndexReference?: string;
    authorizationServiceReference?: string;
    transactionServiceReference?: string;

    operationTracingLinkExclusionList?: any;
    operationTracingLevel?: string;
}
