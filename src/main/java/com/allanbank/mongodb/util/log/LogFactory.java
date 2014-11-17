/*
 * #%L
 * LogFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * LogFactory supports the creation of the Log instances.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class LogFactory {

    /** The {@link LogFactory} instance. */
    private volatile static LogFactory ourInstance;

    /**
     * Creates a {@link Log} instance for the provided class.
     * 
     * @param clazz
     *            The name of the class to create a log instance for.
     * @return The {@link Log} instance for the class.
     */
    public static Log getLog(final Class<?> clazz) {
        return getLog(clazz.getName());
    }

    /**
     * Creates a {@link Log} instance for the provided class.
     * 
     * @param name
     *            The name to create a log instance for.
     * @return The {@link Log} instance for the class.
     */
    public static Log getLog(final String name) {
        LogFactory factory = ourInstance;
        if (factory == null) {
            try {
                factory = new Slf4jLogFactory();
            }
            catch (final RuntimeException e) {
                factory = new JulLogFactory();
            }
            ourInstance = factory;
        }
        return factory.doGetLog(name);
    }

    /**
     * Resets the logger factory to the JUL (Java Util Logging). Provided for
     * testing.
     */
    /* package */static void forceJul() {
        ourInstance = new JulLogFactory();
    }

    /**
     * Resets the logger factory being used. Provided for testing.
     */
    /* package */static void reset() {
        ourInstance = null;
    }

    /**
     * Creates a new {@link LogFactory}.
     */
    protected LogFactory() {
        super();
    }

    /**
     * Delegate method for the instantiated {@link LogFactory}.
     * 
     * @param name
     *            The name of the logger.
     * @return The {@link Log} instance for the class.
     */
    protected abstract Log doGetLog(String name);
}
