/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

/**
 * JulLogFactory provides factory to create {@link JulLog} instances.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JulLogFactory extends LogFactory {
    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a {@link JulLog} instance.
     * </p>
     */
    @Override
    protected Log doGetLog(final Class<?> clazz) {
        return new JulLog(clazz);
    }
}
