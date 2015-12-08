package com.vmware.xenon.samples.querytasks.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;

import java.util.EnumSet;

public class OracleEmployeesService extends StatelessService {
    public static final String SELF_LINK = "/oracle-employees";

    @Override
    public void handleGet(Operation get) {

        QueryTask.Query q = new QueryTask.Query();
        q.setTermPropertyName(PersonService.PersonState.FIELD_NAME_EMAIL);
        q.setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD);
        q.setTermMatchValue("*@oracle*");

        QueryTask.QuerySpecification qs = new QueryTask.QuerySpecification();
        qs.query = q;
        qs.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);

        QueryTask tsk = QueryTask.create(qs);
        tsk.setDirect(true);

        Operation.CompletionHandler c = (o, ex) -> {
            if (ex != null) {
                get.fail(ex);
                return;
            }
            QueryTask rsp = o.getBody(QueryTask.class);
            if (rsp.results != null) {
                get.setBodyNoCloning(rsp.results);
            }
            get.complete();
        };
        // post to the query service
        Operation postQuery = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(tsk)
                .setCompletion(c);

        sendRequest(postQuery);
    }
}
