/*
 * #%L
 * Slf4jLogFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.util.log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Slf4jLogFactory provides factory to create {@link Slf4jLog} instances.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Slf4jLogFactory extends LogFactory {
    /** The <code>LocationAwareLogger</code> class. */
    private final Class<?> myLocationAwareLoggerClass;

    /**
     * The <code>getLogger(Class)</code> method from the
     * <code>LoggerFactory</code> interface.
     */
    private final Method myLogFactoryMethod;

    /**
     * The <code>log(Marker, String, int, String, Object[])</code> method from
     * the <code>LocationAwareLogger</code> interface.
     */
    private final Method myLogMethod;

    /**
     * Creates a new Slf4jLogFactory.
     * 
     * @throws RuntimeException
     *             On a failure to find the SLF4J Logger.
     */
    public Slf4jLogFactory() throws RuntimeException {
        Class<?> logFactoryClass;
        try {
            logFactoryClass = Class.forName("org.slf4j.LoggerFactory");
        }
        catch (final ClassNotFoundException e) {
            // Don't log. SLF4J is not on the classpath to use.
            throw new RuntimeException(e);
        }

        try {
            myLogFactoryMethod = logFactoryClass.getMethod("getLogger",
                    String.class);

            myLocationAwareLoggerClass = Class
                    .forName("org.slf4j.spi.LocationAwareLogger");
            final Class<?> markerClass = Class.forName("org.slf4j.Marker");
            myLogMethod = myLocationAwareLoggerClass.getMethod("log",
                    markerClass, String.class, int.class, String.class,
                    Object[].class, Throwable.class);
        }
        catch (final ClassNotFoundException e) {
            Logger.getLogger(Slf4jLogFactory.class.getName()).log(
                    Level.WARNING,
                    "Failed bootstrap the SLF4J logger: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
        catch (final NoSuchMethodException e) {
            Logger.getLogger(Slf4jLogFactory.class.getName()).log(
                    Level.WARNING,
                    "Failed bootstrap the SLF4J logger: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a {@link Slf4jLog} instance if SLF4J LoggerFactory
     * returns a LocationAwareLogger instance. Otherwise a {@link JulLog} is
     * returned.
     * </p>
     */
    @Override
    protected Log doGetLog(final String name) {
        Log log = null;
        try {
            final Object logger = myLogFactoryMethod.invoke(null, name);
            if (myLocationAwareLoggerClass.isInstance(logger)) {
                log = new Slf4jLog(myLogMethod, logger);
            }
        }
        catch (final IllegalAccessException e) {
            // Fall through.
        }
        catch (final InvocationTargetException e) {
            // Fall through.
        }
        catch (final RuntimeException e) {
            // Fall through.
        }

        // Fall back to JUL logging.
        if (log == null) {
            Logger.getLogger(Slf4jLogFactory.class.getName()).warning(
                    "Falling back to the JUL logger.");
            log = new JulLog(name);
        }

        return log;
    }
}
