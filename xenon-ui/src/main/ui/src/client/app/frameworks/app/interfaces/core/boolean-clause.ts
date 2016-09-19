import { QueryTerm } from '../index';

export interface BooleanClause {
    /**
     * A single term definition.
     *
     * The booleanClauses property must be null if this property is specified.
     */
    term?: QueryTerm;
    
    occurance?: string;

    /**
     * A boolean query definition, composed out multiple sub queries.
     *
     * The term property must be null if this property is specified.
     */
    booleanClauses?: BooleanClause[];
}
