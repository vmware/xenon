package com.vmware.xenon.services.samples;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Sample code showing how to build a "nested" service where a "parent" is a
 * container for zero or more "children" objects.  In this example,
 * we are modeling a universe of dogs and fleas. Every dog has a name.
 * Every flea has a hair-count, and every flea lives on exactly one dog.
 */
public class ParentChildExample {

    /** The Dogs service is a stateless service which accepts four types of paths:
     * - /dogs
     * - /dogs/:did
     * - /dogs/:did/fleas
     * - /dogs/:did/fleas/:fid
     * This service simply forwards all requests to the "internal" services that do
     * all the heavy lifting: the "/internal/dogs" and "/internal/fleas" factories, and
     * the "/internal/dogs/:did" and "/internal/fleas/:fid" stateful services.
     */
    public static class DogsService extends StatelessService {
        public final static String SELF_LINK = "/dogs";
        public final static String FLEAS = "fleas";
        public DogsService() {
            toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        }
        private static enum PathType {
            DOGS("/dogs/?"),
            DOG("/dogs/([^/]+)"),
            FLEAS("/dogs/([^/]+)/fleas/?"),
            FLEA("/dogs/([^/]+)/fleas/([^/]+)");
            private final Pattern pattern;
            PathType(String pattern) {
                this.pattern = Pattern.compile(pattern);
            }
            public static class Pair<T,U> {
                private final T t;
                private final U u;
                public Pair(T t, U u) { this.t = t; this.u = u; };
                public T get1st() { return t; }
                public U get2nd() { return u; }
            }
            public static Pair<PathType,List<String>> forOperation(Operation op) {
                for (PathType type : values()) {
                    Matcher matcher = type.pattern.matcher(op.getUri().getRawPath());
                    if (matcher.matches()) {
                        List<String> ids = new ArrayList<>();
                        for (int i = 0; i < matcher.groupCount(); i++) {
                            ids.add(matcher.group(i + 1));
                        }
                        return new Pair<>(type, ids);
                    }
                }
                throw new IllegalArgumentException();
            }
        }
        @Override public void handleGet(Operation op) {
            forwardOperation(op, Operation::createGet, false);
        }
        @Override public void handlePost(Operation op) {
            forwardOperation(op, Operation::createPost, true);
        }
        @Override public void handlePut(Operation op) {
            forwardOperation(op, Operation::createPut, true);
        }
        @Override public void handlePatch(Operation op) {
            forwardOperation(op, Operation::createPatch, true);
        }
        @Override public void handleDelete(Operation op) {
            forwardOperation(op, Operation::createDelete, false);
        }
        private void forwardOperation(Operation operation, Function<URI,Operation> operationCreator, boolean hasBody) {
            // parse the request so we can route the request to the appropriate internal service
            PathType.Pair<PathType,List<String>> pair = PathType.forOperation(operation);
            PathType pathType = pair.get1st();
            List<String> ids = pair.get2nd();
            String path = null, dogId = null;
            switch (pathType) {
            case DOGS:
                // /dogs ==> /internal/dogs
                path = InternalDogsFactoryService.SELF_LINK;
                break;
            case DOG:
                // /dogs/:did ==> /internal/dogs/:did
                path = UriUtils.buildUriPath(InternalDogsFactoryService.SELF_LINK, ids.get(0));
                break;
            case FLEAS:
                // /dogs/:did/fleas ==> GET/DELETE /internal/fleas?dog-id=:did, POST/PUT/PATCH /internal/fleas with dogLink=/dogs/:did in payload
                path = InternalFleasFactoryService.SELF_LINK;
                dogId = ids.get(0);
                break;
            case FLEA:
                // /dogs/:did/fleas/:fid ==> GET/DELETE /internal/fleas/:fid?dog-id=:did, PUT/PATCH /internal/fleas/:fid with dogLink=/dogs/:did in payload
                path = UriUtils.buildUriPath(InternalFleasFactoryService.SELF_LINK, ids.get(1));
                dogId = ids.get(0);
                break;
            default: throw new IllegalStateException();
            }
            // for operations _*without*_ a body, add a query "?dog=:did" if it exists (ie, for flea operations)
            String query = null;
            if (!hasBody && dogId != null) {
                query = UriUtils.buildUriQuery(InternalFleasFactoryService.DOG_ID, dogId);
            }
            // create the operation
            Operation op =
                operationCreator.
                apply(
                    UriUtils.buildUri(
                        getHost(),
                        path,
                        query)).
                transferRefererFrom(operation).
                transferRequestHeadersFrom(operation).
                setContentType(operation.getContentType()).
                setBody(operation.getBodyRaw()).
                setCompletion(
                    forwarded -> {
                        operation.
                        addResponseHeader(Operation.CONTENT_TYPE_HEADER, forwarded.getContentType()).
                        transferResponseHeadersFrom(forwarded).
                        setBody(forwarded.getBodyRaw()).
                        setStatusCode(forwarded.getStatusCode()).
                        complete();
                    },
                    (forwarded, failure) -> {
                        operation.fail(failure);
                    });
            // for operations _*with*_ a body, put ":did" to the payload
            if (hasBody && dogId != null) {
                // The body must be a flea because dogs don't hit this code path.
                Flea flea = operation.getBody(Flea.class);
                flea.dogLink = UriUtils.buildUriPath(DogsService.SELF_LINK, dogId);
                op.setBody(flea);
            }
            // launch the operation
            sendRequest(op);
        }
    } // end class DogsService

    /**
     * We want to expose a self-links under the "/dogs" endpoint. But the usual
     * documentSelfLink's are under "/internal/{dogs,fleas}".  So our internal
     * services automatically populate a "selfLink".  For example, a flea with
     * documentSelfLink = "/internal/fleas/:fid" and dogLink = "/dogs/:did"
     * will have selfLink = "/dogs/:did/fleas/:fid".
     */
    public static class CustomSelfLinkedServiceDocument extends ServiceDocument {
        public String selfLink;
    }

    /**
     * A dog has a name and zero or more fleas.
     */
    public static class Dog extends CustomSelfLinkedServiceDocument {
        public String name;
        // Note that fleaLinks is dynamically created during a GET; it is always persisted
        // as null, so that we don't need to keep it in sync with the fleas.
        public List<String> fleaLinks;
    }

    /**
     * A flea has a hair-count, and a dog on which it lives.
     */
    public static class Flea extends CustomSelfLinkedServiceDocument {
        public static final String KIND = Utils.buildKind(Flea.class);
        public int nhairs;
        public static final String DOG_LINK = "dogLink";
        public String dogLink;
    }

    /**
     * Factory for the internal dog service
     */
    public static class InternalDogsFactoryService extends FactoryService {
        public static final String SELF_LINK = "/internal/dogs";
        public InternalDogsFactoryService() {
            super(Dog.class);
            toggleOption(ServiceOption.URI_NAMESPACE_OWNER, true);
        }
        @Override
        public Service createServiceInstance() {
            return new InternalDogService();
        }
    }

    /**
     * Internal dog service
     */
    public static class InternalDogService extends StatefulService {
        public InternalDogService() {
            super(Dog.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
        }
        @Override
        public void handleCreate(Operation create) {
            Dog dog = create.getBody(Dog.class);
            dog.selfLink = UriUtils.buildUriPath(DogsService.SELF_LINK, UriUtils.getLastPathSegment(dog.documentSelfLink));
            create.setBody(dog);
            super.handleCreate(create);
        }
        @Override
        public void handleGet(Operation get) {
            // dynamically populate fleaLinks
            Query.Builder query = Query.Builder.create();
            query.addFieldClause(ServiceDocument.FIELD_NAME_KIND, Flea.KIND);
            query.addFieldClause(Flea.DOG_LINK, UriUtils.buildUriPath(DogsService.SELF_LINK, UriUtils.getLastPathSegment(get.getUri().getRawPath())));
            sendRequest(
                Operation.
                createPost(getHost(), ServiceUriPaths.CORE_QUERY_TASKS).
                setBody(QueryTask.Builder.createDirectTask().setQuery(query.build()).build()).
                setCompletion(
                    results -> {
                        Dog dog = getState(get);
                        dog.fleaLinks = new ArrayList<>();
                        ServiceDocumentQueryResult links = results.getBody(QueryTask.class).results;
                        if (links != null && links.documentLinks != null) {
                            for (String fleaLink : links.documentLinks) {
                                dog.fleaLinks.add(UriUtils.buildUriPath(dog.selfLink, DogsService.FLEAS, UriUtils.getLastPathSegment(fleaLink)));
                            }
                        }
                        get.setBody(dog).complete();
                    },
                    (results, failure) -> {
                        log(Level.WARNING, "Failure while querying for fleas: %s", failure);
                        get.fail(Operation.STATUS_CODE_INTERNAL_ERROR);
                    }));
        }
    }

    /**
     * Factory for the internal flea service
     */
    public static class InternalFleasFactoryService extends FactoryService {
        public final static String SELF_LINK = "/internal/fleas";
        public final static String DOG_ID = "dog-id";
        public InternalFleasFactoryService() {
            super(Flea.class);
        }
        @Override
        public Service createServiceInstance() {
            return new InternalFleaService();
        }
    }

    /**
     * Internal flea service
     */
    public static class InternalFleaService extends StatefulService {
        public InternalFleaService() {
            super(Flea.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
        }
        @Override
        public void handleCreate(Operation create) {
            Flea flea = create.getBody(Flea.class);
            flea.selfLink = UriUtils.buildUriPath(flea.dogLink, DogsService.FLEAS, UriUtils.getLastPathSegment(flea.documentSelfLink));
            create.setBody(flea);
            super.handleCreate(create);
        }
    }

}