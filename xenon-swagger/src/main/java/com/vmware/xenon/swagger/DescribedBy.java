package com.vmware.xenon.swagger;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A Xenon service can be annotated with DescribedBy and point the Swagger integration
 * to a JAX-RS compatible description of the service.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DescribedBy {
    /**
     * A class or interface annotated with JAX-RS annotations. The swagger description of the
     * service is built on it.
     * @return
     */
    Class<?> value();
}
