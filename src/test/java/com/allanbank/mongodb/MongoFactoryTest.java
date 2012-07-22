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

/**
 * MongoFactoryTest provides tests for the {@link MongoFactory} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoFactoryTest {

    /**
     * Test method for {@link MongoFactory#create(MongoDbConfiguration)}.
     */
    @Test
    public void testCreateMongoDbConfiguration() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        final Mongo mongo = MongoFactory.create(config);

        assertSame(config, mongo.getConfig());
    }

    /**
     * Test method for {@link MongoFactory#create(String)}.
     */
    @Test
    public void testCreateString() {

        final Mongo mongo = MongoFactory.create("mongodb://localhost");
        final MongoDbConfiguration config = mongo.getConfig();

        assertNotNull(config);
        assertEquals(Collections.singletonList(MongoDbConfiguration
                .parseAddress("localhost")), config.getServers());
    }

}
