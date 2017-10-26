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

import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation.AuthorizationContext;
import com.vmware.xenon.common.OperationProcessingChain.Filter;
import com.vmware.xenon.common.OperationProcessingChain.FilterReturnCode;
import com.vmware.xenon.common.OperationProcessingChain.OperationProcessingContext;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.jwt.Signer;
import com.vmware.xenon.services.common.ServiceHostManagementService;
import com.vmware.xenon.services.common.authn.AuthenticationConstants;
import com.vmware.xenon.services.common.authn.BasicAuthenticationUtils;
import com.vmware.xenon.services.common.authz.AuthorizationConstants;

public class AuthorizationFilter implements Filter {

    private ConcurrentHashMap<String, AuthorizationContext> authorizationContextCache;
    private ConcurrentHashMap<String, Set<String>> userLinkToTokenMap;

    @Override
    public void init() {
        this.authorizationContextCache = new ConcurrentHashMap<>();
        this.userLinkToTokenMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        this.authorizationContextCache.clear();
        this.userLinkToTokenMap.clear();
    }

    public void cacheAuthorizationContext(ServiceHost h, String token, AuthorizationContext ctx) {
        synchronized (this) {
            this.authorizationContextCache.put(token, ctx);
            addUserToken(this.userLinkToTokenMap, ctx.getClaims().getSubject(), token);
        }

        h.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_INSERT_COUNT, 1);
        h.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_SIZE,
                this.authorizationContextCache.size());
    }

    public void clearAuthorizationContext(ServiceHost h, String userLink) {
        synchronized (this) {
            Set<String> tokenSet = this.userLinkToTokenMap.remove(userLink);
            if (tokenSet != null) {
                for (String token : tokenSet) {
                    this.authorizationContextCache.remove(token);
                }
            }
        }

        h.getManagementService().adjustStat(
                ServiceHostManagementService.STAT_NAME_AUTHORIZATION_CACHE_SIZE,
                this.authorizationContextCache.size());
    }

    public AuthorizationContext getAuthorizationContext(String token) {
        return this.authorizationContextCache.get(token);
    }

    public AuthorizationContext createAuthorizationContext(Signer tokenSigner, String userLink) {
        Claims.Builder cb = new Claims.Builder();
        cb.setIssuer(AuthenticationConstants.DEFAULT_ISSUER);
        cb.setSubject(userLink);

        cb.setExpirationTime(Instant.MAX.getEpochSecond());

        // Generate token for set of claims
        Claims claims = cb.getResult();
        String token;
        try {
            token = tokenSigner.sign(claims);
        } catch (GeneralSecurityException e) {
            // This function is run first when the host starts, which will fail if this
            // exception comes up. This is necessary because the host cannot function
            // without having access to the system user's context.
            throw new RuntimeException(e);
        }

        AuthorizationContext.Builder ab = AuthorizationContext.Builder.create();
        ab.setClaims(claims);
        ab.setToken(token);
        ab.setPropagateToClient(false);
        return ab.getResult();
    }

    @Override
    public FilterReturnCode processRequest(Operation op, OperationProcessingContext context) {
        if (!context.getHost().isAuthorizationEnabled() || context.getHost().getAuthorizationService() == null) {
            // authorization is disabled or no authorization service
            return FilterReturnCode.CONTINUE_PROCESSING;
        }

        context.setSuspendConsumer(o -> {
            if (op.getAuthorizationContext() != null) {
                checkAndPopulateAuthzContext(op, context);
            } else {
                populateAuthorizationContext(op, context, (authorizationContext) -> {
                    checkAndPopulateAuthzContext(op, context);
                });
            }
        });

        return FilterReturnCode.SUSPEND_PROCESSING;
    }

    private void checkAndPopulateAuthzContext(Operation op, OperationProcessingContext context) {
        Service authzService = context.getHost().getAuthorizationService();

        long dispatchTime = System.nanoTime();
        op.nestCompletion(o -> {
            if (authzService.hasOption(ServiceOption.INSTRUMENTATION)) {
                long dispatchDuration = System.nanoTime() - dispatchTime;
                AuthUtils.setAuthDurationStat(authzService,
                        AuthorizationConstants.STAT_NAME_DURATION_MICROS_PREFIX,
                        TimeUnit.NANOSECONDS.toMicros(dispatchDuration));
            }

            context.getOpProcessingChain().resumeProcessingRequest(op, context,
                    FilterReturnCode.CONTINUE_PROCESSING, null);
        });

        // TODO: fix AuthenticationContextService and just send it a POST
        context.getHost().queueOrScheduleRequest(authzService, op);
    }

    private void populateAuthorizationContext(Operation op, OperationProcessingContext context,
            Consumer<AuthorizationContext> authorizationContextHandler) {
        ServiceHost host = context.getHost();

        getAuthorizationContext(op, context, authorizationContext -> {
            if (authorizationContext == null) {
                authorizationContext = host.getGuestAuthorizationContext();

                // Check if we have an authorizationContext already setup for the Guest user
                AuthorizationContext cachedGuestCtx = host.getAuthorizationContext(null,
                        authorizationContext.getToken());
                if (cachedGuestCtx != null) {
                    authorizationContext = cachedGuestCtx;
                }
            }

            op.setAuthorizationContext(authorizationContext);
            authorizationContextHandler.accept(authorizationContext);
        });
    }

    private void getAuthorizationContext(Operation op, OperationProcessingContext context, Consumer<AuthorizationContext> authorizationContextHandler) {
        String token = BasicAuthenticationUtils.getAuthToken(op);

        if (token == null) {
            authorizationContextHandler.accept(null);
            return;
        }

        AuthorizationContext ctx = context.getHost().getAuthorizationContext(null, token);
        if (ctx != null) {
            ctx = checkAndGetAuthorizationContext(ctx, ctx.getClaims(), token, op, context);
            if (ctx == null) {
                // Delegate token verification to the authentication service for handling
                // cases like token refresh, etc.
                verifyToken(op, context, authorizationContextHandler);
                return;
            }
            authorizationContextHandler.accept(ctx);
            return;
        }

        verifyToken(op, context, authorizationContextHandler);
    }

    private AuthorizationContext checkAndGetAuthorizationContext(AuthorizationContext ctx,
            Claims claims, String token, Operation op, OperationProcessingContext context) {
        ServiceHost host = context.getHost();

        if (claims == null) {
            host.log(Level.INFO, "Request to %s has no claims found with token: %s",
                    op.getUri().getPath(), token);
            return null;
        }

        Long expirationTime = claims.getExpirationTime();
        if (expirationTime != null && TimeUnit.SECONDS.toMicros(expirationTime) <= Utils.getSystemNowMicrosUtc()) {
            host.log(Level.FINE, "Token expired for %s", claims.getSubject());
            host.clearAuthorizationContext(null, claims.getSubject());
            return null;
        }

        if (ctx != null) {
            return ctx;
        }

        AuthorizationContext.Builder b = AuthorizationContext.Builder.create();
        b.setClaims(claims);
        b.setToken(token);
        ctx = b.getResult();
        host.cacheAuthorizationContext(null, token, ctx);

        return ctx;
    }

    private void verifyToken(Operation op, OperationProcessingContext context,
            Consumer<AuthorizationContext> authorizationContextHandler) {
        ServiceHost host = context.getHost();
        boolean shouldRetry = true;

        URI tokenVerificationUri = host.getAuthenticationServiceUri();
        if (tokenVerificationUri == null) {
            // It is possible to receive a request while the host is starting up: the listener is
            // processing requests but the core authorization/authentication services are not yet
            // started
            host.log(Level.WARNING, "Error verifying token, authentication service not initialized");
            authorizationContextHandler.accept(null);
            return;
        }

        if (host.getBasicAuthenticationServiceUri().equals(host.getAuthenticationServiceUri())) {
            // if authenticationService is BasicAuthenticationService, then no need to retry
            shouldRetry = false;
        }

        verifyTokenInternal(op, context, tokenVerificationUri, authorizationContextHandler, shouldRetry);
    }

    private void verifyTokenInternal(Operation parentOp, OperationProcessingContext context,
            URI tokenVerificationUri, Consumer<AuthorizationContext> authorizationContextHandler,
            boolean shouldRetry) {
        ServiceHost host = context.getHost();
        Operation verifyOp = Operation
                .createPost(tokenVerificationUri)
                .setReferer(parentOp.getUri())
                .transferRequestHeadersFrom(parentOp)
                .setCookies(parentOp.getCookies())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERIFY_TOKEN)
                .setCompletion(
                        (resultOp, ex) -> {
                            if (ex != null) {
                                host.log(Level.WARNING, "Error verifying token: %s", ex);
                                if (shouldRetry) {
                                    ServiceErrorResponse err = resultOp
                                            .getBody(ServiceErrorResponse.class);
                                    // If external authentication fails with this specific error
                                    // code, we can skip basic auth.
                                    if (err.getErrorCode()
                                            == ServiceErrorResponse.ERROR_CODE_EXTERNAL_AUTH_FAILED) {
                                        host.log(Level.FINE, () -> "Skipping basic auth.");
                                        context.getOpProcessingChain().resumeProcessingRequest(parentOp, context,
                                                FilterReturnCode.FAILED_STOP_PROCESSING, ex);
                                        parentOp.transferResponseHeadersFrom(resultOp);
                                        parentOp.fail(resultOp.getStatusCode(),
                                                new RuntimeException(err.message),
                                                resultOp.getBodyRaw());
                                        return;
                                    }
                                    host.log(Level.INFO,
                                            "Retrying token verification with basic auth.");
                                    verifyTokenInternal(parentOp,
                                            context,
                                            host.getBasicAuthenticationServiceUri(),
                                            authorizationContextHandler, false);
                                } else {
                                    authorizationContextHandler.accept(null);
                                }
                            } else {
                                AuthorizationContext ctx = resultOp.getBody(AuthorizationContext.class);
                                // check to see if the subject is valid
                                Operation getUserOp = Operation.createGet(
                                        AuthUtils.buildUserUriFromClaims(host, ctx.getClaims()))
                                        .setReferer(parentOp.getUri())
                                        .setCompletion((getOp, getEx) -> {
                                            if (getEx != null) {
                                                host.log(Level.WARNING, "Error obtaining subject: %s", getEx);
                                                // return a null context. This will result in the auth context
                                                // for this operation defaulting to the guest context
                                                authorizationContextHandler.accept(null);
                                                return;
                                            }
                                            AuthorizationContext authCtx = checkAndGetAuthorizationContext(
                                                    null, ctx.getClaims(), ctx.getToken(), parentOp, context);
                                            parentOp.transferResponseHeadersFrom(resultOp);
                                            Map<String, String> cookies = resultOp.getCookies();
                                            if (cookies != null) {
                                                Map<String, String> parentOpCookies = parentOp
                                                        .getCookies();
                                                if (parentOpCookies == null) {
                                                    parentOp.setCookies(cookies);
                                                } else {
                                                    parentOpCookies.putAll(cookies);
                                                }
                                            }
                                            authorizationContextHandler.accept(authCtx);
                                        });
                                getUserOp.setAuthorizationContext(host.getSystemAuthorizationContext());
                                host.sendRequest(getUserOp);
                            }
                        });
        verifyOp.setAuthorizationContext(host.getSystemAuthorizationContext());
        host.sendRequest(verifyOp);
    }

    private void addUserToken(Map<String, Set<String>> userLinktoTokenMap, String userServiceLink,
            String token) {
        Set<String> tokenSet = userLinktoTokenMap.get(userServiceLink);
        if (tokenSet == null) {
            tokenSet = new HashSet<String>();
        }
        tokenSet.add(token);
        userLinktoTokenMap.put(userServiceLink, tokenSet);
    }
}
