/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.xenon.services.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Pattern;

import com.vmware.xenon.common.ReflectionUtils;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Occurance;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QueryTerm;
import com.vmware.xenon.services.common.QueryTask.QueryTerm.MatchType;

/**
 * The QueryFilter class facilitates testing whether or not a query matches
 * an instance of a {@link com.vmware.xenon.common.ServiceDocument}.
 *
 * It takes a two pass approach where first the query is converted into
 * disjunctive normal form. In the second pass, this DNF is converted into a
 * tree of evaluators that can be called to determine whether or not
 * any of the queries applies to the given document.
 */
public class QueryFilter {
    public static class QueryFilterException extends Exception {
        private static final long serialVersionUID = -1063270176637734896L;

        QueryFilterException(String message) {
            super(message);
        }
    }

    public static class UnsupportedMatchTypeException extends QueryFilterException {
        private static final long serialVersionUID = 4125723225019700727L;

        private static final String FORMAT = "Unsupported matchType: %s";

        UnsupportedMatchTypeException(Term term) {
            super(String.format(FORMAT, String.valueOf(term.term.matchType)));
        }
    }

    public static class UnsupportedPropertyException extends QueryFilterException {
        private static final long serialVersionUID = -7761717288266048287L;

        private static final String FORMAT = "Unsupported property: %s";

        UnsupportedPropertyException(Term term) {
            super(String.format(FORMAT, term.term.propertyName));
        }
    }

    /**
     * Static QueryFilter that always evaluates to true.
     */
    public static final QueryFilter TRUE = new QueryFilter(StaticEvaluator.TRUE);

    /**
     * Static QueryFilter that always evaluates to false.
     */
    public static final QueryFilter FALSE = new QueryFilter(StaticEvaluator.FALSE);

    private final Evaluator evaluator;

    public static QueryFilter create(Query q) throws QueryFilterException {
        List<Conjunction> dnf = createDisjunctiveNormalForm(q);
        Evaluator ev = DisjunctionEvaluator.create(dnf);
        return new QueryFilter(ev);
    }

    private QueryFilter(Evaluator evaluator) {
        this.evaluator = evaluator;
    }

    public boolean evaluate(ServiceDocument document, ServiceDocumentDescription description) {
        return this.evaluator.evaluate(document, description);
    }

    /**
     * Term represents a single term in a {@link Conjunction}.
     *
     * Whether or not a term is negated or not is resolved by walking the original query,
     * which is why it is not possible to use the {@link QueryTerm} directly and why it
     * needs to be wrapped in a class with an extra field.
     */
    static class Term {
        final QueryTerm term;
        final boolean negate;
        final Pattern pattern;

        final List<String> propertyParts;

        Term(QueryTerm term, boolean negate) {
            this.term = term;
            this.negate = negate;

            if (term.matchType == MatchType.WILDCARD) {
                String regex = wildcardToRegex(term.matchValue);
                this.pattern = Pattern.compile(regex);
            } else {
                this.pattern = null;
            }

            // Build collection of parts the propertyName is made up of.
            // Cheaper to build once than to build at run time.
            List<String> tmp = Arrays.asList(this.term.propertyName
                    .split(QueryTask.QuerySpecification.FIELD_NAME_REGEXP));
            this.propertyParts = Collections.unmodifiableList(tmp);
        }

        private static String wildcardToRegex(String wildcard) {
            int len = wildcard.length();
            StringBuilder sb = new StringBuilder(len + 10);
            sb.append('^');
            for (int i = 0; i < len; i++) {
                char c = wildcard.charAt(i);
                switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '+':
                    // TODO workaround for '+' in url query? if yes, need to properly decode url
                    sb.append(".+");
                    break;
                case '?':
                    sb.append('.');
                    break;
                case '(':
                case ')':
                case '[':
                case ']':
                case '$':
                case '^':
                case '.':
                case '{':
                case '}':
                case '|':
                case '\\':
                    sb.append('\\');
                    sb.append(c);
                    break;
                default:
                    sb.append(c);
                    break;
                }
            }
            sb.append('$');
            return sb.toString();
        }
    }

    /**
     * Conjunction is a container class for a set of {@link Term}s.
     *
     * The terms make up a single branch of evaluation in a query, for which the query
     * would evaluate to true if ALL the terms in the conjunction evaluate to true.
     */
    static class Conjunction implements Iterable<Term> {
        /**
         * Array of {@link Term}s that make up this conjunction.
         *
         * This field is intentionally not an {@link ArrayList} since that adds
         * unnecessary memory and compute overhead over a direct array.
         */
        final Term[] terms;

        /**
         * Array of indices in the {@code terms} array to skip when iterating over it.
         *
         * This enables iteration over progressively smaller conjunctions without having
         * to create a new conjunction every time a term needs to be spliced.
         *
         * This field is ignored when extending a conjunction.
         */
        boolean[] skip = null;

        /**
         * Constructor to extend an existing conjunction with an extra {@link Term}.
         *
         * Only used in the DNF creation phase.
         *
         * @param original Conjunction to extend.
         * @param term Term to extend the conjunction with.
         */
        Conjunction(Conjunction original, Term term) {
            Term[] terms;
            if (original != null) {
                terms = Arrays.copyOf(original.terms, original.terms.length + 1);
            } else {
                terms = new Term[1];
            }

            terms[terms.length - 1] = term;
            this.terms = terms;
        }

        /**
         * Mark specified {@link Term} as a term to skip in subsequent calls to
         * this conjunction's iterator.
         *
         * The specified argument is expected to have been returned by this
         * class's iterator. This means that term equality can be checked using
         * the {@code ==} operator instead of calling {@code equals}.
         *
         * @param term Term to skip.
         */
        void skipTerm(Term term) {
            if (this.skip == null) {
                this.skip = new boolean[this.terms.length];
            }

            // Mark specified term as term to be skipped
            for (int i = 0; i < this.terms.length; i++) {
                if (!this.skip[i] && this.terms[i] == term) {
                    this.skip[i] = true;
                    break;
                }
            }
        }

        /**
         * Returns an iterator over this conjunction's {@link Term}s.
         *
         * If the {@code skip} array is non-<tt>null</tt>, the skipped
         * terms are omitted from the terms returned by the iterator.
         *
         * @return iterator
         */
        @Override
        public Iterator<Term> iterator() {
            if (this.skip == null) {
                return new TermIterator(this);
            } else {
                return new TermSkipIterator(this);
            }
        }

        private class TermIterator implements Iterator<Term> {
            protected final Conjunction conjunction;
            protected int i = 0;

            public TermIterator(Conjunction conjunction) {
                this.conjunction = conjunction;
            }

            @Override
            public boolean hasNext() {
                return this.i < this.conjunction.terms.length;
            }

            @Override
            public Term next() throws NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return this.conjunction.terms[this.i++];
            }
        }

        private class TermSkipIterator extends TermIterator {
            public TermSkipIterator(Conjunction conjunction) {
                super(conjunction);
            }

            @Override
            public boolean hasNext() {
                for (; this.i < this.conjunction.terms.length; this.i++) {
                    if (this.conjunction.skip[this.i]) {
                        continue;
                    }
                    return true;
                }
                return false;
            }
        }

        @Override
        public String toString() {
            class TermByPropertyNameComparator implements Comparator<Term> {
                @Override
                public int compare(Term o1, Term o2) {
                    return o1.term.propertyName.compareTo(o2.term.propertyName);
                }
            }

            List<Term> terms = new ArrayList<>();
            for (Term t : this) {
                terms.add(t);
            }

            // Sort by propertyName (to get deterministic output)
            terms.sort(new TermByPropertyNameComparator());

            // Build string of sorted terms
            StringBuilder sb = new StringBuilder();
            for (Term term : terms) {
                if (sb.length() > 0) {
                    sb.append(" AND ");
                }

                if (term.negate) {
                    sb.append("NOT(");
                }

                sb.append(term.term.propertyName);
                sb.append("=");
                sb.append(term.term.matchValue);

                if (term.negate) {
                    sb.append(")");
                }
            }

            return sb.toString();
        }
    }

    static List<Conjunction> createDisjunctiveNormalForm(Query q) {
        ArrayList<Conjunction> prefixes = new ArrayList<>();
        createDisjunctiveNormalForm(q, prefixes, false);
        return prefixes;
    }

    static void createDisjunctiveNormalForm(Query q, ArrayList<Conjunction> prefixes, boolean negate) {
        if (q.term != null) {
            Term t = new Term(q.term, negate);
            if (prefixes.isEmpty()) {
                prefixes.add(new Conjunction(null, t));
            } else {
                // Add term to every prefix
                for (int i = 0; i < prefixes.size(); i++) {
                    prefixes.set(i, new Conjunction(prefixes.get(i), t));
                }
            }
            return;
        }

        // MUST and MUST_NOT makes a set of clauses into a conjunction.
        // Any SHOULD_OCCUR clauses in that set can then be discarded.
        // Apply DeMorgan's laws in the negation case by converting MUST to SHOULD and vice versa.
        int mustClauses = 0;
        int shouldClauses = 0;
        for (Query clause : q.booleanClauses) {
            Occurance o = clause.occurance;
            if (o == null) {
                o = Occurance.MUST_OCCUR;
            }

            switch (o) {
            case MUST_OCCUR:
                if (!negate) {
                    createDisjunctiveNormalForm(clause, prefixes, false);
                }
                mustClauses++;
                break;
            case MUST_NOT_OCCUR:
                if (!negate) {
                    createDisjunctiveNormalForm(clause, prefixes, true);
                }
                mustClauses++;
                break;
            case SHOULD_OCCUR:
                if (negate) {
                    createDisjunctiveNormalForm(clause, prefixes, false);
                }
                shouldClauses++;
                break;
            default:
                throw new RuntimeException("Unknown occurance: " + o.toString());
            }
        }

        // The set of clauses is a disjunction if and only if all of the clauses are effectively
        // SHOULD_OCCUR clauses.
        if ((negate && mustClauses == q.booleanClauses.size())
                || (!negate && shouldClauses == q.booleanClauses.size())) {
            ArrayList<Conjunction> originalPrefixes = new ArrayList<>(prefixes);
            prefixes.clear();

            // Expand prefixes such that for every original prefix, there are N returned prefixes.
            // One for every clause in this disjunction.
            for (Query clause : q.booleanClauses) {
                ArrayList<Conjunction> clausePrefixes = new ArrayList<>(originalPrefixes);
                boolean negateClause = (clause.occurance == Occurance.MUST_NOT_OCCUR) ? false : negate;
                createDisjunctiveNormalForm(clause, clausePrefixes, negateClause);
                prefixes.addAll(clausePrefixes);
            }
        }
    }

    /**
     * Interface for any class that evaluates {@link ServiceDocument}s.
     */
    interface Evaluator {
        boolean evaluate(ServiceDocument document, ServiceDocumentDescription description);
    }

    /**
     * StaticEvaluator returns {@code true} or {@code false} for any document.
     */
    static class StaticEvaluator implements Evaluator {
        static final Evaluator TRUE = new StaticEvaluator(true);
        static final Evaluator FALSE = new StaticEvaluator(false);

        private boolean value;

        private StaticEvaluator(boolean value) {
            this.value = value;
        }

        @Override
        public boolean evaluate(ServiceDocument document, ServiceDocumentDescription description) {
            return this.value;
        }
    }

    /**
     * ConjunctionEvaluator iterates over a collection of terms and checks whether
     * every term matches the specified document. If and only if every term matches,
     * evaluation returns {@code true}.
     */
    static class ConjunctionEvaluator implements Evaluator {
        private final Collection<Term> terms;

        private ConjunctionEvaluator(Collection<Term> terms) {
            this.terms = terms;
        }

        @Override
        public boolean evaluate(ServiceDocument document, ServiceDocumentDescription description) {
            for (Term term : this.terms) {
                String propertyName = term.propertyParts.get(0);
                PropertyDescription pd = description.propertyDescriptions.get(propertyName);
                if (pd == null) {
                    if (!term.negate) {
                        return false;
                    }

                    continue;
                }

                Object o = ReflectionUtils.getPropertyValue(pd, document);
                if (!evaluateTerm(term, o, pd, 1)) {
                    return false;
                }
            }

            return true;
        }

        private boolean evaluateString(Term term, String o) {
            if (term.term.matchType == MatchType.WILDCARD) {
                return term.pattern.matcher(o == null ? "" : o).matches();
            } else if (term.term.matchType == MatchType.PREFIX) {
                return o != null && o.startsWith(term.term.matchValue);
            }
            return Objects.equals(o, term.term.matchValue);
        }

        private boolean evaluateNumber(Term term, Number o) {
            if (term.term.range == null) {
                return false;
            }

            double max = term.term.range.max.doubleValue();
            double min = term.term.range.min.doubleValue();
            double val = o.doubleValue();

            if (term.term.range.isMaxInclusive && val == max
                    || term.term.range.isMinInclusive && val == min
                    || (min < val && val < max)) {
                return true;
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        private boolean evaluateTerm(Term term, Object o, PropertyDescription pd, int depth) {
            if (pd.typeName == TypeName.STRING) {
                if (o != null && !(o instanceof String)) {
                    return term.negate;
                }

                if (term.negate) {
                    if (evaluateString(term, (String) o)) {
                        return false;
                    }
                } else {
                    if (!evaluateString(term, (String) o)) {
                        return false;
                    }
                }
            } else if (pd.typeName == TypeName.LONG || pd.typeName == TypeName.DOUBLE) {
                if (!(o instanceof Number)) {
                    return term.negate;
                }

                if (term.negate) {
                    if (evaluateNumber(term, (Number) o)) {
                        return false;
                    }
                } else {
                    if (!evaluateNumber(term, (Number) o)) {
                        return false;
                    }
                }
            } else if (pd.typeName == TypeName.COLLECTION) {
                if (!(o instanceof Collection)) {
                    return term.negate;
                }

                // Require a part describing the map's key
                if (depth >= term.propertyParts.size()) {
                    return term.negate;
                }

                // This property can only be evaluated if the propertyName includes the right suffix
                String suffix = term.propertyParts.get(depth);
                if (!suffix.equals(QueryTask.QuerySpecification.COLLECTION_FIELD_SUFFIX)) {
                    return false;
                }

                if (pd.elementDescription.typeName == TypeName.STRING) {
                    Collection<String> cs = (Collection<String>) o;
                    if (term.negate) {
                        for (String s : cs) {
                            if (evaluateString(term, s)) {
                                return false;
                            }
                        }
                    } else {
                        boolean found = false;
                        for (String s : cs) {
                            if (evaluateString(term, s)) {
                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            return false;
                        }
                    }
                } else {
                    // Not supported yet...
                    return false;
                }
            } else if (pd.typeName == TypeName.MAP) {
                if (!(o instanceof Map)) {
                    return term.negate;
                }

                // Require a part describing the map's key
                if (depth >= term.propertyParts.size()) {
                    return term.negate;
                }

                Map<String, Object> map = (Map<String, Object>) o;
                String key = term.propertyParts.get(depth);
                Object value = map.get(key);
                return evaluateTerm(term, value, pd.elementDescription, depth + 1);
            } else if (pd.typeName == TypeName.PODO) {
                if (depth >= term.propertyParts.size()) {
                    return term.negate;
                }

                String propertyName = term.propertyParts.get(depth);
                PropertyDescription fd = pd.fieldDescriptions.get(propertyName);
                if (fd == null) {
                    return term.negate;
                }

                Object value = ReflectionUtils.getPropertyValue(fd, o);
                return evaluateTerm(term, value, fd, depth + 1);
            } else {
                // Not supported yet...
                return false;
            }

            return true;
        }

        static Evaluator create(Conjunction conjunction) throws QueryFilterException {
            ArrayList<Term> terms = new ArrayList<>();
            for (Term term : conjunction) {
                if (term.term.range == null &&
                        term.term.matchType != MatchType.TERM &&
                        term.term.matchType != MatchType.WILDCARD &&
                        term.term.matchType != MatchType.PREFIX) {
                    throw new UnsupportedMatchTypeException(term);
                }

                terms.add(term);
            }

            // If there are no terms, evaluation will always succeed.
            if (terms.isEmpty()) {
                return StaticEvaluator.TRUE;
            }

            return new ConjunctionEvaluator(terms);
        }
    }

    /**
     * DisjunctionEvaluator iterates over a collection of evaluators and checks whether
     * any evaluator evaluates to {@code true} for the specified document. Evaluation
     * returns as soon as the first {@link Evaluator} evaluates to {@code true}.
     */
    static class DisjunctionEvaluator implements Evaluator {
        private final Collection<Evaluator> evaluators;

        private DisjunctionEvaluator(Collection<Evaluator> evaluators) {
            this.evaluators = evaluators;
        }

        @Override
        public boolean evaluate(ServiceDocument document, ServiceDocumentDescription description) {
            for (Evaluator e : this.evaluators) {
                if (e.evaluate(document, description)) {
                    return true;
                }
            }
            return false;
        }

        static Evaluator create(Collection<Conjunction> dnf) throws QueryFilterException {
            ArrayList<Evaluator> evaluators = new ArrayList<>();

            while (!dnf.isEmpty()) {
                String key = findTopPropertyForDispatch(dnf);
                if (key == null) {
                    break;
                }

                // Create dispatch evaluator for top key.
                // The dnf collection is mutated to only hold the conjunctions
                // that are not handled by the newly created dispatch evaluator.
                Evaluator e = DispatchEvaluator.create(key, dnf);
                evaluators.add(e);
            }

            // Add remaining conjunctions as direct conjunction evaluators
            for (Conjunction conjunction : dnf) {
                evaluators.add(ConjunctionEvaluator.create(conjunction));
            }

            return new DisjunctionEvaluator(evaluators);
        }
    }

    /**
     * DispatchEvaluator contains a dispatch table from property values to evaluators.
     * It is bound to a specific property and for that property looks up the specified
     * document's value. If the value is present in the dispatch table, evaluation is
     * deferred to the associated evaluator. If the value is not present in the dispatch
     * table, evaluation returns {@code false}.
     *
     * Presence of the document's property value in the dispatch table is equivalent to
     * a term that requires the specified property to equal that value.
     */
    static class DispatchEvaluator implements Evaluator {
        private final String propertyName;
        private final Map<String, Evaluator> table;

        private DispatchEvaluator(String propertyName, Map<String, Evaluator> table) {
            this.propertyName = propertyName;
            this.table = table;
        }

        @Override
        public boolean evaluate(ServiceDocument document, ServiceDocumentDescription description) {
            PropertyDescription pd = description.propertyDescriptions.get(this.propertyName);
            if (pd == null) {
                return false;
            }

            Object propValue = ReflectionUtils.getPropertyValue(pd, document);
            String matchAs = QuerySpecification.toMatchValue(propValue);
            if (matchAs == null) {
                return false;
            }

            Evaluator e = this.table.get(matchAs);
            if (e == null) {
                return false;
            }

            return e.evaluate(document, description);
        }

        /**
         * Creates a dispatch evaluator.
         *
         * This function uses only the conjunctions in the {@code dnf} that contain
         * a term that constrains the specified property. When this function returns,
         * the {@code dnf} collection only contains conjunctions NOT handled by the
         * newly created dispatch evaluator (i.e. it is mutated).
         *
         * The values in the newly created dispatch table are created as disjunction
         * evaluators themselves. This is where the {@link Evaluator} tree creation
         * process recurses.
         *
         * @param propertyName property to create dispatch table for.
         * @param dnf query in disjunctive normal form.
         * @return DispatchEvaluator
         * @throws QueryFilterException
         */
        static Evaluator create(String propertyName, Collection<Conjunction> dnf)
                throws QueryFilterException {
            Collection<Conjunction> unhandled = new ArrayList<>();
            Map<String, Collection<Conjunction>> table = new HashMap<>();

            for (Conjunction conjunction : dnf) {
                boolean handled = false;

                for (Term term : conjunction) {
                    if (!isTermEligibleForDispatch(term)) {
                        continue;
                    }

                    if (!term.term.propertyName.equals(propertyName)) {
                        continue;
                    }

                    // Find collection of conjunctions for this value
                    Collection<Conjunction> entry = table.get(term.term.matchValue);
                    if (entry == null) {
                        entry = new ArrayList<>();
                        table.put(term.term.matchValue, entry);
                    }

                    // Mark term as skipped; it is now handled by the dispatch table
                    conjunction.skipTerm(term);

                    // Add conjunction to list of conjunctions in dispatch table
                    entry.add(conjunction);
                    handled = true;
                    break;
                }

                if (!handled) {
                    unhandled.add(conjunction);
                }
            }

            Map<String, Evaluator> evaluatorTable = new HashMap<>();
            for (Entry<String, Collection<Conjunction>> e : table.entrySet()) {
                evaluatorTable.put(e.getKey(), DisjunctionEvaluator.create(e.getValue()));
            }

            // Reset dnf argument to only hold the conjunctions were not
            // added to the dispatch table.
            dnf.clear();
            dnf.addAll(unhandled);

            return new DispatchEvaluator(propertyName, evaluatorTable);
        }
    }

    /**
     * Find most constrained property.
     *
     * Ordering disjunction evaluation from most used to least used properties means
     * that evaluation can typically return sooner for passing documents.
     *
     * @param dnf query in disjunctive normal form.
     * @return name of most frequently constrained property.
     */
    private static String findTopPropertyForDispatch(Collection<Conjunction> dnf) {
        class Elem {
            final String name;
            int count;

            Elem(String name) {
                this.name = name;
            }

            void add() {
                this.count++;
            }
        }

        class ElemComparator implements Comparator<Elem> {
            @Override
            public int compare(Elem o1, Elem o2) {
                return o1.count - o2.count;
            }
        }

        Map<String, Elem> elemByProperty = new HashMap<>();
        for (Conjunction conjunction : dnf) {
            for (Term term : conjunction) {
                if (!isTermEligibleForDispatch(term)) {
                    continue;
                }

                Elem e = elemByProperty.get(term.term.propertyName);
                if (e == null) {
                    e = new Elem(term.term.propertyName);
                    elemByProperty.put(term.term.propertyName, e);
                }

                e.add();
            }
        }

        if (elemByProperty.isEmpty()) {
            return null;
        }

        // Convert values to array and sort by count
        Elem[] elements = elemByProperty.values().toArray(new Elem[elemByProperty.size()]);
        Arrays.sort(elements, new ElemComparator());
        return elements[elements.length - 1].name;
    }

    private static boolean isTermNestedProperty(Term term) {
        return term.propertyParts.size() > 1;
    }

    private static boolean isTermEligibleForDispatch(Term term) {
        // Ignore negations
        if (term.negate) {
            return false;
        }

        // Ignore non-TERM matches
        if (term.term.matchType != MatchType.TERM) {
            return false;
        }

        // Ignore nested properties
        if (isTermNestedProperty(term)) {
            return false;
        }

        return true;
    }
}
