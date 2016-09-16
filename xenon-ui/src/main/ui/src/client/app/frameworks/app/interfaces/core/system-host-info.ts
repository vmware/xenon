export interface SystemHostInfo {
    properties: {[key: string]: string};
    environmentVariables: {[key: string]: string};
    availableProcessorCount: number;
    freeMemoryByteCount: number;
    totalMemoryByteCount: number;
    maxMemoryByteCount: number;
    freeDiskByteCount: number;
    usableDiskByteCount: number;
    totalDiskByteCount: number;
    ipAddresses: string[];
    osName: string;
    osFamily: string;
}
