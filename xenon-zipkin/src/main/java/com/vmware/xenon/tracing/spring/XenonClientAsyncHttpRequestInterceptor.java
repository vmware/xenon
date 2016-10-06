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

package com.vmware.xenon.tracing.spring;

import java.io.IOException;
import java.net.URI;

import com.github.kristofa.brave.ClientRequestInterceptor;
import com.github.kristofa.brave.ClientResponseInterceptor;
import com.github.kristofa.brave.NoAnnotationsClientResponseAdapter;
import com.github.kristofa.brave.http.HttpClientRequest;
import com.github.kristofa.brave.http.HttpClientRequestAdapter;
import com.github.kristofa.brave.http.HttpClientResponseAdapter;
import com.github.kristofa.brave.http.HttpResponse;
import com.github.kristofa.brave.http.SpanNameProvider;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.AsyncClientHttpRequestExecution;
import org.springframework.http.client.AsyncClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

/**
 * Http Request interceptors for Spring based services which will submit spans to Zipkin.
 */
public class XenonClientAsyncHttpRequestInterceptor implements AsyncClientHttpRequestInterceptor {

    private final ClientRequestInterceptor requestInterceptor;
    private final ClientResponseInterceptor responseInterceptor;
    private final SpanNameProvider spanNameProvider;

    /**
     * Constructor.
     *
     * @param requestInterceptor request interceptor
     * @param responseInterceptor response interceptor
     * @param spanNameProvider span name provider
     */
    public XenonClientAsyncHttpRequestInterceptor(final ClientRequestInterceptor requestInterceptor,
            final ClientResponseInterceptor responseInterceptor,
            final SpanNameProvider spanNameProvider) {
        this.requestInterceptor = requestInterceptor;
        this.responseInterceptor = responseInterceptor;
        this.spanNameProvider = spanNameProvider;
    }

    @Override
    public ListenableFuture<ClientHttpResponse> intercept(HttpRequest request, byte[] body,
            AsyncClientHttpRequestExecution execution)
            throws IOException {

        this.requestInterceptor
                .handle(new HttpClientRequestAdapter(new SpringHttpClientRequest(request),
                        this.spanNameProvider));

        ListenableFuture<ClientHttpResponse> future = execution.executeAsync(request, body);
        future.addCallback(
                new HttpResponseSuccessCallBack<ClientHttpResponse>(this.responseInterceptor),
                new HttpResponseFailureCallBack(this.responseInterceptor));
        return future;
    }

    /**
     * Wrapper class for Success Callback.
     * @param <C>
     */
    static class HttpResponseSuccessCallBack<C> implements SuccessCallback<ClientHttpResponse> {
        private ClientResponseInterceptor responseInterceptor;

        public HttpResponseSuccessCallBack(ClientResponseInterceptor responseInterceptor) {
            this.responseInterceptor = responseInterceptor;
        }

        @Override
        public void onSuccess(ClientHttpResponse resp) {
            try {
                this.responseInterceptor
                        .handle(new HttpClientResponseAdapter(
                                new SpringHttpResponse(resp.getRawStatusCode())));
            } catch (RuntimeException | IOException up) {
                // Ignore the failure of not being able to get the status code from the response;
                // let the calling code find out themselves
                this.responseInterceptor.handle(NoAnnotationsClientResponseAdapter.getInstance());
            }
        }
    }

    /**
     * Wrapper class for Failure Callback.
     */
    static class HttpResponseFailureCallBack implements FailureCallback {
        private ClientResponseInterceptor responseInterceptor;

        public HttpResponseFailureCallBack(ClientResponseInterceptor responseInterceptor) {
            this.responseInterceptor = responseInterceptor;
        }

        @Override
        public void onFailure(Throwable ex) {
            // process error
            this.responseInterceptor.handle(NoAnnotationsClientResponseAdapter.getInstance());
        }
    }

    /**
     * Wrapper class.
     */
    static class SpringHttpClientRequest implements HttpClientRequest {

        private final HttpRequest request;

        /**
         * Constructor.
         *
         * @param request request
         */
        SpringHttpClientRequest(final HttpRequest request) {
            this.request = request;
        }

        @Override
        public void addHeader(final String header, final String value) {
            this.request.getHeaders().add(header, value);
        }

        @Override
        public URI getUri() {
            return this.request.getURI();
        }

        @Override
        public String getHttpMethod() {
            return this.request.getMethod().name();
        }
    }

    /**
     * Wrapper class.
     */
    static class SpringHttpResponse implements HttpResponse {

        private final int status;

        /**
         * Constructor.
         *
         * @param status status
         */
        SpringHttpResponse(final int status) {
            this.status = status;
        }

        @Override
        public int getHttpStatusCode() {
            return this.status;
        }
    }
}

