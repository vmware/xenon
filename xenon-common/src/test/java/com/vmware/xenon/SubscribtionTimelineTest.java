package com.vmware.xenon;

import static org.junit.Assert.assertNull;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleTaskService;
import com.vmware.xenon.services.common.ExampleTaskService.ExampleTaskServiceState;
import com.vmware.xenon.services.common.ReliableSubscriptionService;

public class SubscribtionTimelineTest extends BasicTestCase {

    public static Logger logger = Logger.getLogger(SubscribtionTimelineTest.class.getName());

    @Before
    public void before() {
        host.startFactory(new ExampleTaskService());
    }

    @Test
    public void test() {
        int i = 0;
        while (true) {
            logger.info("Iterration " + i);
            Operation initialOp = postExample();

            AtomicReference<Throwable> failure = new AtomicReference<>(null);
            TestContext tc = new TestContext(1, Duration.ofSeconds(60));

            subscribeToNotifications((o) -> {
                //This triggers on notification
                ExampleTaskServiceState receivedState = o
                        .getBody(ExampleTaskService.ExampleTaskServiceState.class);
                logger.info("Notification received for state " + receivedState.subStage);
                Operation getOp = getExample(receivedState.documentSelfLink);
                ExampleTaskServiceState getResult = getOp
                        .getBody(ExampleTaskService.ExampleTaskServiceState.class);

                if (!getResult.subStage.equals(receivedState.subStage)) {
                    String msg = String.format("State mismatch: Expected %s , Got %s", receivedState.subStage, getResult.subStage);
                    logger.severe(msg);
                    failure.set(new AssertionError(msg));
                } else {
                    logger.info("States match");
                }

                if (receivedState.taskInfo.stage.equals(TaskStage.FINISHED)) {
                    tc.complete();
                }
            }, (e) -> {
                logger.severe("This won't get triggered if the subscription creation passes");
            }, initialOp
                    .getBody(ExampleTaskService.ExampleTaskServiceState.class).documentSelfLink);

            tc.await();
            assertNull(failure.get());
        }
    }

    public Operation postExample() {
        AtomicReference<Throwable> failure = new AtomicReference<>(null);
        AtomicReference<Operation> o = new AtomicReference<>(null);

        TestContext tc = new TestContext(1, Duration.ofSeconds(10));

        host.send(
                Operation.createPost(UriUtils.buildUri(host, ExampleTaskService.FACTORY_LINK))
                        .setBody(new ExampleTaskService.ExampleTaskServiceState())
                        .setCompletion((completedOp, ex) -> {
                            o.set(completedOp);
                            if (ex != null) {
                                failure.set(ex);
                            }
                            tc.complete();
                        }));

        tc.await();

        Assert.assertNull(failure.get());
        return o.get();
    }

    public Operation getExample(String link) {
        AtomicReference<Throwable> failure = new AtomicReference<>(null);
        AtomicReference<Operation> o = new AtomicReference<>(null);

        TestContext tc = new TestContext(1, Duration.ofSeconds(10));

        host.send(
                Operation.createGet(UriUtils.buildUri(host, link))
                        .setCompletion((completedOp, ex) -> {
                            o.set(completedOp);
                            if (ex != null) {
                                failure.set(ex);
                            }
                            tc.complete();
                        }));

        tc.await();

        Assert.assertNull(failure.get());
        return o.get();
    }

    public void subscribeToNotifications(Consumer<Operation> onSuccessConsumer,
            Consumer<Throwable> onFailureConsumer,
            String taskLink) {
        ServiceSubscriber subscribeBody = new ServiceSubscriber();
        subscribeBody.replayState = true;
        subscribeBody.usePublicUri = true;
        Operation subscribeOp = Operation
                .createPost(host, taskLink)
                .setReferer(host.getUri())
                .setCompletion((regOp, regEx) -> {
                    if (regEx != null) {
                        onFailureConsumer.accept(regEx);
                    }
                });
        ReliableSubscriptionService notificationTarget = ReliableSubscriptionService.create(
                subscribeOp, subscribeBody, onSuccessConsumer);
        host.startSubscriptionService(subscribeOp, notificationTarget, subscribeBody);
    }
}
