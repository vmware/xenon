import * as moment from 'moment';
import * as numeral from 'numeral';

import { DocumentLink } from '../interfaces/index';

export class StringUtil {
    /**
     * Converts a string into a valid HTML id attribute [A-Za-z0-9_-]
     * Useful for producing readable window.location.hash fragments
     * From: https://gist.github.com/chrisjacob/421217
     *
     * @param {string} s - string to encode
     * @param {string} prefix - to be affixed before the encoded string - HTML id attributes must start with an [A-Za-z] char
     * @param {string} exclude - regex string of characters that should not be encoded
     * @param {string} spacer - the character that joins encoded items
     * @param {object} replace - object of post-encoded strings to str.replace e.g. {'-47-' = '_'} replaces all encoded '/' chars with a '_'
     */

    static encodeToId(s: string, prefix?: string, exclude?: string, spacer?: string, replace?: any): string {
        if (typeof (s) !== 'string') {
            return null;
        }

        prefix = typeof (prefix) !== 'undefined' ? prefix : 'id_'; // use '' to not add any prefix
        exclude = typeof (exclude) !== 'undefined' ? exclude : '[^A-Za-z0-9]'; // use '.' to encode every character
        spacer = typeof (spacer) !== 'undefined' ? spacer : '-'; // use '' to not add any spacer
        replace = typeof (replace) !== 'undefined' ? replace : { '-47-': '_' }; // use FALSE to not make any replacements

        // encode chars
        var regxExclude = new RegExp(exclude, 'g');
        s = s.replace(regxExclude, function(matched) {
            return spacer + matched.charCodeAt(0) + spacer;
        });

        // replace some special chars
        if (replace) {
            for (let i in replace) {
                let regxReplace = new RegExp(i, 'g');
                s = s.replace(regxReplace, replace[i]);
            }
        }

        // add the prefix and return
        return prefix + s;
    }

    /**
     * Converts an encoded string back to it's original string
     * From: https://gist.github.com/chrisjacob/421217
     *
     * @param {string} s - string to decode
     * @param {string} prefix - to be removed from the string
     * @param {string} spacer - the character that joins encoded items
     * @param {object} replace - object of strings to str.replace e.g. {'_' = '/'} replaces all '_' chars with a '/'
     */
    static decodeFromId(s: string, prefix?: string, spacer?: string, replace?: any): string {
        if (typeof (s) !== 'string') {
            return null;
        }

        prefix = typeof (prefix) !== 'undefined' ? prefix : 'id_';
        spacer = typeof (spacer) !== 'undefined' ? spacer : '-';
        replace = typeof (replace) !== 'undefined' ? replace : { '_': '/' };

        var spacerLength = spacer.length;

        // remove prefix
        s = prefix ? s.slice(prefix.length) : s;

        // replace some special chars
        if (replace) {
            for (let i in replace) {
                let regxReplace = new RegExp(i, 'g');
                s = s.replace(regxReplace, replace[i]);
            }
        }

        // decode chars
        var regxCharCode = new RegExp(spacer + '\\d+' + spacer, 'g');
        s = s.replace(regxCharCode, function(matched) {
            return String.fromCharCode(parseInt(matched.slice(spacerLength, matched.length - spacerLength)));
        });

        return s;
    }

    /**
     * Given a document link string, return the document link object.
     *
     * @param {string} documentLink - documentLink to get id and type from
     */
    static parseDocumentLink(documentLink: string): DocumentLink {
        // TODO: need to do a regex check to make sure it's a valid format.
        var linkFragments: string[] = documentLink.split('/');

        return {
            prefix: linkFragments[1],
            type: linkFragments[2],
            id: linkFragments[3]
        };
    }

    /**
     * Return a formatted timestamp string for the given time in microseconds.
     *
     * @param {number} timeInMicroseconds - number of microseconds since epoch.
     * @param {boolean} timeOnly - display only the time not the date.
     */
    static getTimeStamp(timeInMicroseconds: number, timeOnly: boolean = false): string {
        if (timeInMicroseconds === 0) {
            return 'Never';
        }

        var format: string = timeOnly ? 'hh:mm A' : 'M/D/YY hh:mm A';
        return moment(timeInMicroseconds / 1000).local().format(format);
    }

    /**
     * Return a formatted timestamp string for the given time string.
     * Note that since the timeString is already based on local time, no need
     * to call local() like getTimeStamp function does.
     *
     * @param {string} timeString - a string that represents time.
     * @param {boolean} timeOnly - display only the time not the date.
     */
    static formatTimeStamp(timeString: string, timeOnly: boolean = false): string {
        var format: string = timeOnly ? 'hh:mm A' : 'M/D/YY hh:mm A';
        return moment.utc(timeString).format(format);
    }

    /**
     * Return a duration in seconds
     *
     * @param {number} microseconds - a number that represents duration in microseconds.
     */
    static formatDurationToSeconds(microseconds: number): string {
        return StringUtil.formatNumber(moment.duration(microseconds / 1000).asSeconds());
    }

    /**
     * Return a duration in seconds
     *
     * @param {number} microseconds - a number that represents duration in microseconds.
     */
    static formatDurationToMilliseconds(microseconds: number): string {
        return StringUtil.formatNumber(moment.duration(microseconds / 1000).asMilliseconds());
    }

    /**
     * Format a number that represents disk space in bytes into a readable string followed by the closest unit
     * (KB, MB, etc).
     *
     * @param {number} bytes - an integer that represents disk space in bytes.
     */
    static formatDataSize(bytes: number): string {
        var numeralValue = numeral(bytes);

        return numeralValue.format('0.00 b');
    }

    /**
     * Format a number with commas as thousands separators.
     *
     * @param {number} n - an integer that represents disk space in bytes.
     */
    static formatNumber(n: number): string {
        var numeralValue = numeral(n);

        return numeralValue.format('0,0');
    }

    /**
     * Format a number with commas as thousands separators.
     *
     * @param {number} n - an integer that represents disk space in bytes.
     */
    static formatPercentage(n: number): string {
        var numeralValue = numeral(n);

        return numeralValue.format('0.000 %');
    }
}
