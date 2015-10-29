/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.common.http.netty;

import io.netty.channel.ChannelPipeline;

/**
 * Netty channel pipeline initializer can be used to augment Netty pipeline with custom handlers, such as websocket
 * endpoints, CORS support, etc...
 * <p>
 * Pipeline initializer for {@link com.vmware.dcp.common.ServiceHost} may be defined via {@link
 * com.vmware.dcp.common.ServiceHost#setChannelPipelineInitializer(ChannelPipelineInitializer)}.
 */
@FunctionalInterface
public interface ChannelPipelineInitializer {
    /**
     * This method is invoked each time Netty channel is registered.
     *
     * @param pipeline Channel pipeline to augment. Pipeline already contains request encoder/decoder and ssl handler if
     *                 necessary and after calling this method final DCP client request handler will be added to the
     *                 pipeline.
     */
    void initPipeline(ChannelPipeline pipeline);
}
