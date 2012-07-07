/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.junit.Assert.*;

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
        MongoDbConfiguration config = new MongoDbConfiguration();

        Mongo mongo = MongoFactory.create(config);

        assertSame(config, mongo.getConfig());
    }

    /**
     * Test method for {@link MongoFactory#create(String)}.
     */
    @Test
    public void testCreateString() {

        Mongo mongo = MongoFactory.create("mongodb://localhost");
        MongoDbConfiguration config = mongo.getConfig();

        assertNotNull(config);
        assertEquals(Collections.singletonList(MongoDbConfiguration
                .parseAddress("localhost")), config.getServers());
    }

}
