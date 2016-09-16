import { ServiceStatsLogHistogram, ServiceStatsTimeSeries } from '../index';

export interface ServiceStatsEntry {
    /**
         * Name of the stat.
         */
        name: string;

        /**
         * Latest value of the stat.
         */
        latestValue: number;

        /**
         * The value accumulated over time for the stat.
         */
        accumulatedValue: number;

        /**
         * The stat document version.
         */
        version: number;

        /**
         * Time, in microseconds since UNIX epoch, the stat was received at the service.
         */
        lastUpdateMicrosUtc: number;

        /**
         * The kind of document, in this case the ServiceStat kind.
         */
        kind: string;

        /**
         * The unit of measurement associated with the stat.
         */
        unit?: string;

        /**
         * Time, in microseconds since UNIX epoch, the data value was acquired at the source.
         */
        sourceTimeMicrosUtc?: number;

        /**
         * Source (provider) for this stat
         */
        serviceReference?: string;

        /**
         *  histogram of logarithmic time scale
         */
        logHistogram?: ServiceStatsLogHistogram;

        /**
         * time series data. If set,  the stat value is
         * maintained over time. Users can choose the number
         * of samples to maintain and the time window into
         * which each datapoint falls. If more than one entry
         * exists, aggregates (average, minimum, maximum; based
         * on user choice) are maintained
         */
        timeSeriesStats?: ServiceStatsTimeSeries;
}
