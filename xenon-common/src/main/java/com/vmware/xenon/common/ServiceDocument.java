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

package com.vmware.xenon.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.EnumSet;

import com.vmware.xenon.common.Service.Action;

/**
 * Base implementation class for Service documents. A service document is a PODO (data only object,
 * no methods) that describes the service state. Even dynamic, state less services can have a
 * document representing state-less computation results, or data gathered dynamically from other
 * services
 */
public class ServiceDocument {
    public enum DocumentRelationship {
        IN_CONFLICT,
        EQUAL,
        NEWER_VERSION,
        NEWER_UPDATE_TIME,
        EQUAL_TIME,
        EQUAL_VERSION, PREFERRED
    }

    /**
     * Specifies {@link com.vmware.xenon.common.ServiceDocument} field usage option. This annotation is repeatable.
     *
     * @see com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Repeatable(value = UsageOptions.class)
    @Target(ElementType.FIELD)
    public @interface UsageOption {
        /**
         * Field usage option to apply.
         */
        ServiceDocumentDescription.PropertyUsageOption option();
    }

    /**
     * This annotation defines field usage options.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface UsageOptions {
        /**
         * Field usage options to apply.
         */
        UsageOption[] value();
    }

    /**
     * Annotations for ServiceDocumentDescription
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Documentation {
        // sets propertyDocumentation
        String description() default "";

        // sets exampleValue
        String exampleString() default "";
    }

    public static final String FIELD_NAME_SELF_LINK = "documentSelfLink";
    public static final String FIELD_NAME_VERSION = "documentVersion";
    public static final String FIELD_NAME_EPOCH = "documentEpoch";
    public static final String FIELD_NAME_KIND = "documentKind";
    public static final String FIELD_NAME_UPDATE_TIME_MICROS = "documentUpdateTimeMicros";
    public static final String FIELD_NAME_UPDATE_ACTION = "documentUpdateAction";
    public static final String FIELD_NAME_DESCRIPTION = "documentDescription";
    public static final String FIELD_NAME_OWNER = "documentOwner";
    public static final String FIELD_NAME_SOURCE_LINK = "documentSourceLink";
    public static final String FIELD_NAME_EXPIRATION_TIME_MICROS = "documentExpirationTimeMicros";
    public static final String FIELD_NAME_AUTH_PRINCIPAL_LINK = "documentAuthPrincipalLink";
    public static final String FIELD_NAME_TRANSACTION_ID = "documentTransactionId";

    /**
     * Field names ending with this suffix will be indexed as URI paths
     */
    public static final String FIELD_NAME_SUFFIX_LINK = "Link";

    /**
     * Field names ending in Address will be indexed as StringFields.
     */
    public static final String FIELD_NAME_SUFFIX_ADDRESS = "Address";

    /**
     * Optional description of the document, populated by the framework on requests to
     * <service>/template
     */
    public ServiceDocumentDescription documentDescription;

    /**
     * Monotonically increasing number indicating the number of updates the local service instance
     * has processed in the current host session or since creation, if the service is durable
     *
     * Infrastructure use only
     */
    public long documentVersion;

    /**
     * Monotonically increasing number associated with a document owner. Each time ownership
     * changes a protocol guarantees new owner will assign a higher epoch number and get consensus
     * between peers before proceeding with replication.
     *
     * This field is not used for non replicated services
     *
     * Infrastructure use only
     */
    public Long documentEpoch;

    /**
     * A structured string identifier for the document PODO type
     *
     * Infrastructure use only
     */
    public String documentKind;

    /**
     * The relative URI path of the service managing this document
     */
    public String documentSelfLink;

    /**
     * Document update time in microseconds since UNIX epoch
     *
     * Infrastructure use only
     */
    public long documentUpdateTimeMicros;

    /**
     * Update action that produced the latest document version
     *
     * Infrastructure use only
     */
    public String documentUpdateAction;

    /**
     * Expiration time in microseconds since UNIX epoch. If a document is found to be expired a
     * running service instance will be deleted and the document will be marked deleted in the index
     */
    public long documentExpirationTimeMicros;

    /**
     * Identity of the node that is assigned this document. Assignment is done through the node
     * group partitioning service and it can change between versions
     *
     * Infrastructure use only
     */
    public String documentOwner;

    /**
     * Link to the document that served as the source for this document. The source document is
     * retrieved during a logical clone operation when this instance was created through a POST to a
     * service factory
     */
    public String documentSourceLink;

    /**
     * The relative URI path of the user principal who owns this document
     *
     * Infrastructure use only
     */
    public String documentAuthPrincipalLink;

    /**
     * Refers to the transaction coordinator: if this particular state version is part of a
     * pending transaction whose coordinator is `/core/transactions/[txid]`, it lets the system
     * hide this version from queries, concurrent transactions and operations -- if they don't
     * explicitly provide this id.
     */
    public String documentTransactionId;

    public void copyTo(ServiceDocument target) {
        target.documentEpoch = this.documentEpoch;
        target.documentDescription = this.documentDescription;
        target.documentOwner = this.documentOwner;
        target.documentSourceLink = this.documentSourceLink;
        target.documentVersion = this.documentVersion;
        target.documentKind = this.documentKind;
        target.documentSelfLink = this.documentSelfLink;
        target.documentUpdateTimeMicros = this.documentUpdateTimeMicros;
        target.documentExpirationTimeMicros = this.documentExpirationTimeMicros;
        target.documentAuthPrincipalLink = this.documentAuthPrincipalLink;
        target.documentTransactionId = this.documentTransactionId;
    }

    public static boolean isDeleted(ServiceDocument document) {
        if (document == null) {
            return false;
        }
        return Action.DELETE.toString().equals(document.documentUpdateAction);
    }

    /**
     * Infrastructure use only.
     * Compares two documents and returns an set describing the relationship between them.
     *
     * All relationship flags are relative to the first parameter (stateA). The timeEpsilon is used
     * to determine if the update times between the two documents happened too close to each other
     * to safely determine order.
     *
     * If document A is preferred, the PREFERRED option will be set
     */
    public static EnumSet<DocumentRelationship> compare(ServiceDocument stateA,
            ServiceDocument stateB,
            ServiceDocumentDescription desc,
            long timeEpsilon) {

        boolean preferred = false;
        EnumSet<DocumentRelationship> results = EnumSet.noneOf(DocumentRelationship.class);

        if (stateB == null) {
            results.add(DocumentRelationship.PREFERRED);
            return results;
        }

        if (stateA.documentVersion > stateB.documentVersion) {
            results.add(DocumentRelationship.NEWER_VERSION);
            preferred = true;
        } else if (stateA.documentVersion == stateB.documentVersion) {
            results.add(DocumentRelationship.EQUAL_VERSION);
        }

        if (stateA.documentUpdateTimeMicros == stateB.documentUpdateTimeMicros) {
            results.add(DocumentRelationship.EQUAL_TIME);
        } else if (stateA.documentUpdateTimeMicros > stateB.documentUpdateTimeMicros) {
            results.add(DocumentRelationship.NEWER_UPDATE_TIME);
        }

        // TODO We only attempt conflict detection if versions match. Otherwise we pick the highest
        // version which is not always correct. We need to formalize the eventual consistency model
        // and do proper merges for divergent states (due to partitioning, etc). Dotted version
        // vectors and other schemes are relatively easy to implement using our existing tracking
        // data
        if (results.contains(DocumentRelationship.EQUAL_VERSION)) {
            if (results.contains(DocumentRelationship.NEWER_UPDATE_TIME)) {
                preferred = true;
            } else if (Math.abs(stateA.documentUpdateTimeMicros
                    - stateB.documentUpdateTimeMicros) < timeEpsilon) {
                // documents changed within time epsilon so we can not safely determine which one
                // is newer.
                if (!ServiceDocument.equals(desc, stateA, stateB)) {
                    results.add(DocumentRelationship.IN_CONFLICT);
                }
            }
        }

        if (preferred) {
            results.add(DocumentRelationship.PREFERRED);
        }

        return results;
    }

    /**
     * Compares ServiceDocument for equality. If they are same, then this method returns true;
     * false otherwise.
     *
     * @param description     ServiceDocumentDescription obtained by calling getDocumentTemplate
     *                        ().documentDescription
     * @param currentDocument current service document instance
     * @param newDocument     new service document instance
     * @return true / false
     */
    public static boolean equals(ServiceDocumentDescription description,
            ServiceDocument currentDocument, ServiceDocument newDocument) {
        try {
            if (currentDocument == null || newDocument == null) {
                throw new IllegalArgumentException(
                        "Null Service documents cannot be checked for equality.");
            }

            String currentSignature = Utils.computeSignature(currentDocument, description);
            String newSignature = Utils.computeSignature(newDocument, description);
            return currentSignature.equals(newSignature);
        } catch (Throwable throwable) {
            if (throwable instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) throwable;
            }
            return false;
        }
    }

    /**
     * Returns whether or not the {@code name} is a built-in field or not.
     *
     * @param name Field name
     * @return true/false
     */
    public static boolean isBuiltInDocumentField(String name) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_KIND:
        case ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS:
        case ServiceDocument.FIELD_NAME_OWNER:
        case ServiceDocument.FIELD_NAME_SOURCE_LINK:
        case ServiceDocument.FIELD_NAME_VERSION:
        case ServiceDocument.FIELD_NAME_EPOCH:
        case ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS:
        case ServiceDocument.FIELD_NAME_UPDATE_ACTION:
        case ServiceDocument.FIELD_NAME_SELF_LINK:
        case ServiceDocument.FIELD_NAME_DESCRIPTION:
        case ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK:
        case ServiceDocument.FIELD_NAME_TRANSACTION_ID:
            return true;
        default:
            return false;
        }
    }

    public static boolean isBuiltInIndexedDocumentField(String name) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_KIND:
        case ServiceDocument.FIELD_NAME_EXPIRATION_TIME_MICROS:
        case ServiceDocument.FIELD_NAME_OWNER:
        case ServiceDocument.FIELD_NAME_SOURCE_LINK:
        case ServiceDocument.FIELD_NAME_VERSION:
        case ServiceDocument.FIELD_NAME_EPOCH:
        case ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS:
        case ServiceDocument.FIELD_NAME_SELF_LINK:
        case ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK:
        case ServiceDocument.FIELD_NAME_TRANSACTION_ID:
            return true;
        default:
            return false;
        }
    }

    public static boolean isBuiltInNonIndexedDocumentField(String name) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_DESCRIPTION:
        case ServiceDocument.FIELD_NAME_UPDATE_ACTION:
            return true;
        default:
            return false;
        }
    }

    public static boolean isBuiltInDocumentFieldWithNullExampleValue(String name) {
        switch (name) {
        case ServiceDocument.FIELD_NAME_AUTH_PRINCIPAL_LINK:
        case ServiceDocument.FIELD_NAME_SOURCE_LINK:
        case ServiceDocument.FIELD_NAME_OWNER:
        case ServiceDocument.FIELD_NAME_TRANSACTION_ID:
        case ServiceDocument.FIELD_NAME_EPOCH:
            return true;
        default:
            return false;
        }
    }

    public static boolean isLink(String name) {
        return name.endsWith(FIELD_NAME_SUFFIX_LINK);
    }

}
