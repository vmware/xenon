package com.vmware.xenon.services.samples;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.samples.ParentChildExample.Dog;
import com.vmware.xenon.services.samples.ParentChildExample.DogsService;
import com.vmware.xenon.services.samples.ParentChildExample.Flea;
import com.vmware.xenon.services.samples.ParentChildExample.InternalDogsFactoryService;
import com.vmware.xenon.services.samples.ParentChildExample.InternalFleasFactoryService;

public class TestParentChildExample extends BasicReusableHostTestCase {

    private static final Class<?> SERVICES[] = {
        DogsService.class,
        InternalDogsFactoryService.class,
        InternalFleasFactoryService.class
    };

    @Before
    public void setUp() {
        try {
            String[] servicePaths = new String[SERVICES.length];
            int i = 0;
            for (Class<?> service : SERVICES) {
                host.startService((Service) service.newInstance());
                servicePaths[i++] = service.getDeclaredField(UriUtils.FIELD_NAME_SELF_LINK).get(null).toString();
            }
            TestContext context = TestContext.create(SERVICES.length, TimeUnit.MINUTES.toMicros(1));
            host.registerForServiceAvailability(
                context.getCompletion(),
                servicePaths);
            context.await();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test POST /internal/dogs to create a new dog, then GET the dog
     * to make sure it was created correctly.
     */
    @Test
    public void testCanCreateInternalDog() throws Throwable {
        host.testStart(1);
        // create a dog
        host.sendRequest(
            Operation.
            createPost(host, InternalDogsFactoryService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"name\":\"fido\"}").
            setCompletion(
                post -> {
                    // get the dog
                    host.sendRequest(
                        Operation.
                        createGet(UriUtils.buildUri(host, post.getBody(Dog.class).documentSelfLink)).
                        setReferer(host.getUri()).
                        setCompletion(
                            get -> {
                                Dog dog = get.getBody(Dog.class);
                                if (dog.selfLink != null &&
                                    dog.selfLink.equals(UriUtils.buildUriPath(DogsService.SELF_LINK, UriUtils.getLastPathSegment(dog.documentSelfLink))) &&
                                    dog.name != null &&
                                    dog.name.equals("fido") &&
                                    dog.fleaLinks != null &&
                                    dog.fleaLinks.isEmpty())
                                {
                                    host.completeIteration();
                                } else {
                                    host.failIteration(new RuntimeException("Unexpected selfLink, name or fleaLinks"));
                                }
                            },
                            host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

    /**
     * Test POST /internal/fleas to create a flea on a particular dog (which doesn't actually exist),
     * and then GET the flea we just created.
     */
    @Test
    public void testCanCreateInternalFlea() throws Throwable {
        host.testStart(1);
        // create a flea
        host.sendRequest(
            Operation.
            createPost(host, InternalFleasFactoryService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"nhairs\":10, \"dogLink\":\"/dogs/foobar\"}").
            setCompletion(
                post -> {
                    // get the flea
                    host.sendRequest(
                        Operation.
                        createGet(UriUtils.buildUri(host, post.getBody(Flea.class).documentSelfLink)).
                        setReferer(host.getUri()).
                        setCompletion(
                                get -> {
                                    Flea flea = get.getBody(Flea.class);
                                    if (flea.nhairs == 10 &&
                                        flea.dogLink != null &&
                                        flea.dogLink.equals("/dogs/foobar"))
                                    {
                                        host.completeIteration();
                                    } else {
                                        host.failIteration(new RuntimeException("Unexpected nhairs or dogLink"));
                                    }
                                },
                                host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

    /**
     * Continuation of "testCanCreateInternalFlea" above: Test POST /internal/fleas to create a flea on a
     * phantom dog, and then verify that we can query for its fleas and get back the flea we created.
     */
    @Test
    public void testCanCreateInternalFleaThenQueryForIt() throws Throwable {
        host.testStart(1);
        // create a flea
        host.sendRequest(
            Operation.
            createPost(host, InternalFleasFactoryService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"nhairs\":10, \"dogLink\":\"/dogs/foobar\"}").
            setCompletion(
                post -> {
                    // get the flea
                    host.sendRequest(
                        Operation.
                        createGet(UriUtils.buildUri(host, post.getBody(Flea.class).documentSelfLink)).
                        setReferer(host.getUri()).
                        setCompletion(
                            get -> {
                                Flea flea = get.getBody(Flea.class);
                                if (flea.nhairs == 10 &&
                                    flea.dogLink != null &&
                                    flea.dogLink.equals("/dogs/foobar"))
                                {
                                    // query for all sibling fleas (should get just the one flea)
                                    Query.Builder query = Query.Builder.create();
                                    query.addFieldClause(ServiceDocument.FIELD_NAME_KIND, Flea.KIND);
                                    query.addFieldClause(Flea.DOG_LINK, "/dogs/foobar");
                                    host.sendRequest(
                                        Operation.
                                        createPost(host, ServiceUriPaths.CORE_QUERY_TASKS).
                                        setReferer(host.getUri()).
                                        setBody(QueryTask.Builder.createDirectTask().setQuery(query.build()).build()).
                                        setCompletion(
                                            (results, failure) -> {
                                                if (failure == null) {
                                                    ServiceDocumentQueryResult links = results.getBody(QueryTask.class).results;
                                                    if (links != null &&
                                                        links.documentLinks != null &&
                                                        links.documentLinks.size() == 1 &&
                                                        links.documentLinks.get(0).equals(flea.documentSelfLink))
                                                    {
                                                        host.completeIteration();
                                                    } else {
                                                        host.failIteration(new RuntimeException("Unexpected links"));
                                                    }
                                                } else {
                                                    host.failIteration(failure);
                                                }
                                            }));
                                } else {
                                    host.failIteration(new RuntimeException("Unexpected nhairs or dogLink"));
                                }
                            },
                            host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

    /**
     * Test POST /dogs, then GET the dog to verify it was created correctly.
     */
    @Test
    public void testCanCreateAndGetDog() throws Throwable {
        host.testStart(1);
        // create a dog
        host.sendRequest(
            Operation.
            createPost(host, DogsService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"name\":\"fido\"}").
            setCompletion(
                post -> {
                    // get the dog
                    String dogLink = post.getBody(Dog.class).selfLink;
                    host.sendRequest(
                        Operation.
                        createGet(UriUtils.buildUri(host, dogLink)).
                        setReferer(host.getUri()).
                        setCompletion(
                            get -> {
                                // verify that the dog was created properly
                                Dog dog = get.getBody(Dog.class);
                                if (dog.name != null &&
                                    dog.name.equals("fido") &&
                                    dog.selfLink != null &&
                                    dog.selfLink.equals(dogLink) &&
                                    dog.fleaLinks != null &&
                                    dog.fleaLinks.isEmpty())
                                {
                                    host.completeIteration();
                                } else {
                                    host.failIteration(new RuntimeException("Unexpected name"));
                                }
                            },
                            host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

    /**
     * Test POST /dogs and then POST /dogs/:did/fleas to create a flea on the dog.
     */
    @Test
    public void testCreateAndGetDogWithFleas() throws Throwable {
        host.testStart(1);
        // create a dog
        host.sendRequest(
            Operation.
            createPost(host, DogsService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"name\":\"fido\"}").
            setCompletion(
                postDog -> {
                    String dogLink = postDog.getBody(Dog.class).selfLink;
                    // create a flea on the dog
                    host.sendRequest(
                        Operation.
                        createPost(host, UriUtils.buildUriPath(dogLink, DogsService.FLEAS)).
                        setReferer(host.getUri()).
                        setBody("{\"nhairs\":10}").
                        setCompletion(
                            postFlea -> {
                                // get the dog
                                host.sendRequest(
                                    Operation.
                                    createGet(UriUtils.buildUri(host, dogLink)).
                                    setReferer(host.getUri()).
                                    setCompletion(
                                        getDog -> {
                                            // verify that the dog was created properly
                                            Dog dog = getDog.getBody(Dog.class);
                                            if (dog.name != null &&
                                                dog.name.equals("fido") &&
                                                dog.selfLink != null &&
                                                dog.selfLink.equals(dogLink) &&
                                                dog.fleaLinks != null &&
                                                dog.fleaLinks.size() == 1 &&
                                                dog.fleaLinks.get(0).equals(postFlea.getBody(Flea.class).selfLink))
                                            {
                                                // get the dog's flea
                                                host.sendRequest(
                                                    Operation.
                                                    createGet(host, dog.fleaLinks.get(0)).
                                                    setReferer(host.getUri()).
                                                    setCompletion(
                                                        getFlea -> {
                                                            Flea flea = getFlea.getBody(Flea.class);
                                                            if (flea.nhairs == 10 &&
                                                                    flea.selfLink != null &&
                                                                    flea.selfLink.equals(dog.fleaLinks.get(0)) &&
                                                                    flea.dogLink != null &&
                                                                    flea.dogLink.equals(dog.selfLink))
                                                            {
                                                                host.completeIteration();
                                                            } else {
                                                                host.failIteration(new RuntimeException("Unexpected nhairs, selfLink or dogLinks"));
                                                            }
                                                        },
                                                        host.getCompletion()));
                                            } else {
                                                host.failIteration(new RuntimeException("Unexpected name, selfLink or fleaLinks"));
                                            }
                                        },
                                        host.getCompletion()));
                            },
                            host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

    @Test
    public void testCreateAndGetDogWith5Fleas() throws Throwable {
        host.testStart(1);
        // create a dog
        host.sendRequest(
            Operation.
            createPost(host, DogsService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"name\":\"fido\"}").
            setCompletion(
                postDog -> {
                    String dogLink = postDog.getBody(Dog.class).selfLink;
                    // create 5 fleas on the dog
                    AtomicInteger counter = new AtomicInteger(5);
                    for (int i = 0; i < 5; i++) {
                        host.sendRequest(
                            Operation.
                            createPost(host, UriUtils.buildUriPath(dogLink, DogsService.FLEAS)).
                            setReferer(host.getUri()).
                            setBody("{\"nhairs\":10}").
                            setCompletion(
                                postFlea -> {
                                    if (counter.decrementAndGet() < 1) {
                                        // all fleas created, now get the dog
                                        host.sendRequest(
                                            Operation.
                                            createGet(UriUtils.buildUri(host, dogLink)).
                                            setReferer(host.getUri()).
                                            setCompletion(
                                                getDog -> {
                                                    // verify that the dog has all 5 fleas
                                                    Dog dog = getDog.getBody(Dog.class);
                                                    if (dog.name != null &&
                                                        dog.name.equals("fido") &&
                                                        dog.selfLink != null &&
                                                        dog.selfLink.equals(dogLink) &&
                                                        dog.fleaLinks != null &&
                                                        dog.fleaLinks.size() == 5)
                                                    {
                                                        host.completeIteration();
                                                    } else {
                                                        host.failIteration(new RuntimeException("Unexpected name, selfLink or fleaLinks"));
                                                    }
                                                },
                                                host.getCompletion()));
                                    }
                                },
                                host.getCompletion()));
                    }
                },
                host.getCompletion()));
        host.testWait();
    }

    @Test
    public void testCreateAndGetDeleteDog() throws Throwable {
        host.testStart(1);
        // create a dog
        host.sendRequest(
            Operation.
            createPost(host, DogsService.SELF_LINK).
            setReferer(host.getUri()).
            setBody("{\"name\":\"fido\"}").
            setCompletion(
                postDog -> {
                    // delete the dog we just created
                    String dogLink = postDog.getBody(Dog.class).selfLink;
                    host.sendRequest(
                        Operation.
                        createDelete(host, dogLink).
                        setReferer(host.getUri()).
                        setCompletion(
                            deleteDog -> {
                                host.completeIteration();
                            },
                            host.getCompletion()));
                },
                host.getCompletion()));
        host.testWait();
    }

}
