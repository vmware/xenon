package com.vmware.xenon.samples.time.services;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatelessService;

import java.util.Date;

/**
 * Created by icarrero on 10/1/15.
 */
public class TimeService extends StatelessService {

    public static final String SELF_LINK = "/time";

    @Override
    public void handleGet(Operation get) {
        get.setBody(new Date().toString()).complete();
    }
}
