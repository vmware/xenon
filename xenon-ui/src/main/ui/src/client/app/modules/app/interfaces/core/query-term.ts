import { NumericRange } from '../index';

export interface QueryTerm {
    propertyName: string;
    propertyType?: string;
    matchValue?: string;
    matchType?: string;
    range?: NumericRange;
}
