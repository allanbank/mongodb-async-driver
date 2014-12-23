/*
 * #%L
 * MongoFactoryTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Collections;

import org.junit.Test;

import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * MongoFactoryTest provides tests for the {@link MongoFactory} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoFactoryTest {

    /**
     * Test method for
     * {@link MongoFactory#createClient(MongoClientConfiguration)}.
     */
    @Test
    public void testCreateClientMongoClietConfiguration() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final MongoClient mongo = MongoFactory.createClient(config);

        assertSame(config, mongo.getConfig());
    }

    /**
     * Test method for {@link MongoFactory#createClient(String)}.
     */
    @Test
    public void testCreateClientString() {

        final MongoClient mongo = MongoFactory
                .createClient("mongodb://localhost");
        final MongoClientConfiguration config = mongo.getConfig();

        assertNotNull(config);
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize("localhost")), config.getServers());
    }

    /**
     * Test method for {@link MongoFactory#create(MongoDbConfiguration)}.
     */
    @Deprecated
    @Test
    public void testCreateMongoDbConfiguration() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        final MongoClient mongo = MongoFactory.create(config);

        assertSame(config, mongo.getConfig());
    }

    /**
     * Test method for {@link MongoFactory#create(String)}.
     */
    @Deprecated
    @Test
    public void testCreateString() {

        final Mongo mongo = MongoFactory.create("mongodb://localhost");
        final MongoDbConfiguration config = mongo.getConfig();

        assertNotNull(config);
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize("localhost")), config.getServers());
    }

}
