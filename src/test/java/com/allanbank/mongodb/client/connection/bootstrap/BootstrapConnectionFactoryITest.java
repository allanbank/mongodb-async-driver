/*
 * #%L
 * BootstrapConnectionFactoryITest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.bootstrap;

import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.client.connection.auth.AuthenticationConnectionFactory;
import com.allanbank.mongodb.client.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.client.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;

/**
 * Integration test for the {@link BootstrapConnectionFactory}.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactoryITest extends ServerTestDriverSupport {

    /** The factory being tested. */
    private BootstrapConnectionFactory myTestFactory;

    /**
     * Stops all of the background processes started.
     */
    @After
    public void tearDown() {
        myTestFactory = null;

        stopReplicaSet();
        stopSharded();
        stopStandAlone();
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapReplicaSet() {
        startReplicaSet();

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(ReplicaSetConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapSharded() {
        startSharded();

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(ShardedConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandalone() {
        startStandAlone();

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(SocketConnectionFactory.class));
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#bootstrap()} .
     */
    @Test
    public void testBootstrapStandaloneWithAuth() {
        startAuthenticated();

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        config.addCredential(Credential.builder().userName(USER_NAME)
                .password(PASSWORD).database(USER_DB)
                .authenticationType(Credential.MONGODB_CR).build());

        myTestFactory = new BootstrapConnectionFactory(config);

        assertThat("Wrong type of myTestFactory.", myTestFactory.getDelegate(),
                CoreMatchers.instanceOf(AuthenticationConnectionFactory.class));
    }
}
