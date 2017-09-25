/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 *
 */
public class MigrationBroadcastService extends StatelessService {

    public static final String SELF_LINK = UriUtils.buildUriPath("new-migration");

    public static class MigrationBroadcastState extends ServiceDocument {
        public String factoryPath;
        public Class<? extends ServiceDocument> stateType;
        public String nodeSelectorPath;
        public List<? extends ServiceDocument> documents;
        // TODO: list of replication result for response
    }

    private MigrationBroadcastState sampleData() {
        MigrationBroadcastState state = new MigrationBroadcastState();
        state.factoryPath = "/core/examples";
        state.stateType = ExampleServiceState.class;
        state.nodeSelectorPath = ServiceUriPaths.DEFAULT_NODE_SELECTOR;

        ExampleServiceState exampleFoo = new ExampleServiceState();
        exampleFoo.name = "foo";
        exampleFoo.documentSelfLink = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, exampleFoo.name);
        exampleFoo.documentUpdateAction = Action.PATCH.toString();
        exampleFoo.documentOwner = getHost().getId();


        List<ExampleServiceState> docs = new ArrayList<>();
        docs.add(exampleFoo);

        for (int i = 0; i < 10; i++) {
            ExampleServiceState doc = new ExampleServiceState();
            doc.name = "foo-" + (i + 1);
            doc.documentSelfLink = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, doc.name);
            docs.add(doc);
        }

        state.documents = docs;

        return state;
    }

    @Override
    public void handleGet(Operation get) {
        // just for debugging
        Operation.createPost(this, SELF_LINK).setBody(sampleData())
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        get.fail(ex);
                        return;
                    }
                    get.complete();
                })
                .sendWith(this);
    }

    public static final String HEADER_NEW_MIGRATION = "new-migration";

    @Override
    public void handlePost(Operation post) {

        MigrationBroadcastState body = post.getBody(MigrationBroadcastState.class);
        String factoryPath = body.factoryPath;
//        Class<? extends ServiceDocument> stateType = body.stateType;
        String nodeSelectorPath = body.nodeSelectorPath;

        // TODO: retrieve service options

        for (ServiceDocument state : body.documents) {

            CompletionHandler selectOwnerCh = (selectOwnerOp, selectOwnerEx) -> {
                // TODO: for error case

                SelectOwnerResponse rsp = selectOwnerOp.getBody(SelectOwnerResponse.class);
                String docOwner = rsp.ownerNodeId;

                System.out.println(format("link=%s owner=%s", state.documentSelfLink, rsp.ownerNodeId));

                // create a replication request
                Operation replicationPost = Operation.createPost(UriUtils.buildUri(getHost(), factoryPath))
                        .setCompletion((op, ex) -> {
                            // TODO: if failed
                            post.complete();
                        })
                        .setReferer(getUri())
                        .addPragmaDirective(HEADER_NEW_MIGRATION)
                        .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL)
                        .setExpiration(Utils.fromNowMicrosUtc(TimeUnit.DAYS.toMicros(1)));  // TODO: expiration

                // replication request needs linked-state
                setState(replicationPost, state);

                // send replication request including self node
                getHost().replicateRequest(EnumSet.noneOf(ServiceOption.class), state, nodeSelectorPath,
                        state.documentSelfLink, replicationPost, SelectAndForwardRequest.MIGRATION_OPTIONS, docOwner);
            };


            Operation selectOwnerOp = Operation.createPost(null)
                    .setExpiration(Utils.fromNowMicrosUtc(TimeUnit.DAYS.toMicros(1)))  // TODO: expiration
                    .setCompletion(selectOwnerCh);
            getHost().selectOwner(nodeSelectorPath, state.documentSelfLink, selectOwnerOp);
        }
    }
}
