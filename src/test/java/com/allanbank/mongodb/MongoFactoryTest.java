/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
    @SuppressWarnings("deprecation")
    @Test
    public void testCreateMongoDbConfiguration() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        final MongoClient mongo = MongoFactory.create(config);

        assertSame(config, mongo.getConfig());
    }

    /**
     * Test method for {@link MongoFactory#create(String)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCreateString() {

        final Mongo mongo = MongoFactory.create("mongodb://localhost");
        final MongoDbConfiguration config = mongo.getConfig();

        assertNotNull(config);
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize("localhost")), config.getServers());
    }

}
