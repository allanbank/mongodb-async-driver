/*
 * #%L
 * JmxDatabaseMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics.jmx;

import com.allanbank.mongodb.client.metrics.basic.BasicCollectionMetrics;
import com.allanbank.mongodb.client.metrics.basic.BasicDatabaseMetrics;

/**
 * JmxDatabaseMetrics provides metrics for a single database.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxDatabaseMetrics extends BasicDatabaseMetrics {

    /** The JMX support class. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxConnectionMetrics.
     * 
     * @param support
     *            JMX support class.
     * @param databaseName
     *            The name of database.
     */
    public JmxDatabaseMetrics(final JmxSupport support,
            final String databaseName) {
        super(databaseName);

        mySupport = support;
    }

    /**
     * Removes the database from JMX.
     */
    @Override
    public void close() {
        super.close();
        mySupport.unregister("database", getName());
    }

    /**
     * Registers with the MBean server.
     */
    public void register() {
        mySupport.register(this, "database", getName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to register the JmxCollectionMetrics.
     * </p>
     */
    @Override
    protected void addedCollection(final BasicCollectionMetrics metrics) {
        if (metrics instanceof JmxCollectionMetrics) {
            ((JmxCollectionMetrics) metrics).register();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new JmxCollectionMetrics.
     * </p>
     */
    @Override
    protected BasicCollectionMetrics createMetrics(final String collection) {
        return new JmxCollectionMetrics(mySupport, getName(), collection);
    }
}