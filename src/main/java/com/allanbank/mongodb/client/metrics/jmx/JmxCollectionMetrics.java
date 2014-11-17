/*
 * #%L
 * JmxCollectionMetrics.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * JmxCollectionMetrics provides metrics for a single collection.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxCollectionMetrics extends BasicCollectionMetrics {

    /** The name of the database. */
    private final String myDatabaseName;

    /** The JMX support class. */
    private final JmxSupport mySupport;

    /**
     * Creates a new JmxConnectionMetrics.
     * 
     * @param support
     *            JMX support class.
     * @param databaseName
     *            The name of database.
     * @param collectionName
     *            The name of collection.
     */
    public JmxCollectionMetrics(final JmxSupport support,
            final String databaseName, final String collectionName) {
        super(collectionName);

        mySupport = support;
        myDatabaseName = databaseName;
    }

    /**
     * Removes the collection from JMX.
     */
    @Override
    public void close() {
        super.close();
        mySupport.unregister("collection", myDatabaseName + "." + getName());
    }

    /**
     * Registers with the MBean server.
     */
    public void register() {
        mySupport
                .register(this, "collection", myDatabaseName + "." + getName());
    }
}