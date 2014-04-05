/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
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
        return factory.doGetLog(clazz);
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
     * @param clazz
     *            The name of the class to create a log instance for.
     * @return The {@link Log} instance for the class.
     */
    protected abstract Log doGetLog(Class<?> clazz);
}
