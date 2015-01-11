/*
 * #%L
 * ServerTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.state;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.Client;

/**
 * ServerTest provides tests for the {@link Server} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerTest {

    /**
     * Test method for {@link Server#getAverageLatency()}.
     */
    @Test
    public void testGetAverageLatency() {

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        assertEquals(Double.MAX_VALUE, server.getAverageLatency(), 0.001);
        server.updateAverageLatency(10000000);
        assertEquals(10.0, server.getAverageLatency(), 0.1);
        for (int i = 0; i < Server.DECAY_SAMPLES; ++i) {
            server.updateAverageLatency(15000000);
        }
        assertEquals(15, server.getAverageLatency(), 1.0);
    }

    /**
     * Test method for {@link Server#isWritable()}.
     */
    @Test
    public void testIsWritable() {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.push("tags").addInteger("f", 1);

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        // Default is unknown.
        assertThat(server.getState(), is(Server.State.UNKNOWN));

        // Set to primary.
        builder.reset();
        builder.add("ismaster", true);
        server.update(builder.build());
        assertThat(server.getState(), is(Server.State.WRITABLE));

        // Set to secondary.
        builder.reset();
        builder.add("ismaster", false);
        builder.add("secondary", true);
        server.update(builder.build());
        assertThat(server.getState(), is(Server.State.READ_ONLY));

        // Set to unknown.
        builder.reset();
        builder.add("ismaster", false);
        builder.addNull("secondary");
        server.update(builder.build());
        assertThat(server.getState(), is(Server.State.UNAVAILABLE));

        // Set to primary, again.
        builder.reset();
        builder.add("ismaster", true);
        server.update(builder.build());
        assertThat(server.getState(), is(Server.State.WRITABLE));

        // A document without an is master field has no effect.
        builder.reset();
        server.update(builder.build());
        assertThat(server.getState(), is(Server.State.WRITABLE));
    }

    /**
     * Test method for {@link Server#toString}.
     */
    @Test
    public void testToString() {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.push("tags").addInteger("f", 1);
        builder.add("ismaster", true);

        final Server state = new Server(new InetSocketAddress("foo", 27017));

        assertEquals("foo:27017(UNKNOWN,1.7976931348623157E308)",
                state.toString());

        state.updateAverageLatency(1230000);
        state.update(builder.build());

        assertEquals("foo:27017(WRITABLE,T,1.23)", state.toString());

        builder.reset();
        builder.add("ismaster", false);
        builder.add("secondary", true);
        builder.push("tags");
        state.update(builder.build());

        assertEquals("foo:27017(READ_ONLY,1.23)", state.toString());
    }

    /**
     * Test method for {@link Server#getTags()}.
     */
    @Test
    public void testUpdateWithMaxBatchedWriteOperations() {

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add(Server.MAX_BATCHED_WRITE_OPERATIONS_PROP, 123456);

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        // Default is null/empty.
        assertThat(server.getMaxBatchedWriteOperations(),
                is(Server.MAX_BATCHED_WRITE_OPERATIONS_DEFAULT));

        // Set the tags.
        server.update(builder.build());
        assertThat(server.getMaxBatchedWriteOperations(), is(123456));

        // Clear the tags.
        builder.reset();
        builder.addNull(Server.MAX_BATCHED_WRITE_OPERATIONS_PROP);
        server.update(builder.build());
        assertThat(server.getMaxBatchedWriteOperations(), is(123456));

        // Set them again.
        builder.reset();
        builder.add(Server.MAX_BATCHED_WRITE_OPERATIONS_PROP, 654321L);
        server.update(builder.build());
        assertThat(server.getMaxBatchedWriteOperations(), is(654321));

        // A document without tags has no effect (still set).
        builder.reset();
        server.update(builder.build());
        assertThat(server.getMaxBatchedWriteOperations(), is(654321));
    }

    /**
     * Test method for {@link Server#getTags()}.
     */
    @Test
    public void testUpdateWithMaxBsonObjectSize() {

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add(Server.MAX_BSON_OBJECT_SIZE_PROP, 123456);

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        // Default is null/empty.
        assertThat(server.getMaxBsonObjectSize(), is(Client.MAX_DOCUMENT_SIZE));

        // Set the tags.
        server.update(builder.build());
        assertThat(server.getMaxBsonObjectSize(), is(123456));

        // Clear the tags.
        builder.reset();
        builder.addNull(Server.MAX_BSON_OBJECT_SIZE_PROP);
        server.update(builder.build());
        assertThat(server.getMaxBsonObjectSize(), is(123456));

        // Set them again.
        builder.reset();
        builder.add(Server.MAX_BSON_OBJECT_SIZE_PROP, 654321L);
        server.update(builder.build());
        assertThat(server.getMaxBsonObjectSize(), is(654321));

        // A document without tags has no effect (still set).
        builder.reset();
        server.update(builder.build());
        assertThat(server.getMaxBsonObjectSize(), is(654321));
    }

    /**
     * Test method for {@link Server#getTags()}.
     */
    @Test
    public void testUpdateWithTags() {

        final DocumentBuilder builder = BuilderFactory.start();
        builder.push("tags").addInteger("f", 1);

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        // Default is null/empty.
        assertNull(server.getTags());

        // Set the tags.
        server.update(builder.build());
        assertThat(server.getTags(),
                is(BuilderFactory.start().addInteger("f", 1).build()));

        // Clear the tags.
        builder.reset();
        builder.push("tags");
        server.update(builder.build());
        assertThat(server.getTags(), nullValue());

        // Set them again.
        builder.reset();
        builder.push("tags").addInteger("f", 1);
        server.update(builder.build());
        assertThat(server.getTags(),
                is(BuilderFactory.start().addInteger("f", 1).build()));

        // A document without tags has no effect (still set).
        builder.reset();
        server.update(builder.build());
        assertThat(server.getTags(),
                is(BuilderFactory.start().addInteger("f", 1).build()));
    }

    /**
     * Test method for {@link Server#getTags()}.
     */
    @Test
    public void testUpdateWithVersion() {

        final DocumentBuilder builder = BuilderFactory.start();
        builder.pushArray("versionArray").add(1).add(2L).add(3.0);

        final Server server = new Server(new InetSocketAddress("foo", 27017));

        // Default is "unknown".
        assertThat(server.getVersion(), is(Version.UNKNOWN));

        // Set the version.
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("1.2.3")));

        // Clear the version.
        builder.reset();
        builder.pushArray("versionArray");
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("1.2.3")));

        // Set them again.
        builder.reset();
        builder.pushArray("versionArray").add(2).add(3L).add(4.0);
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("2.3.4")));

        // A document without version has no effect (still set).
        builder.reset();
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("2.3.4")));

        // A document with a string version updates.
        builder.reset();
        builder.add("version", "0.1.2");
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("0.1.2")));

        // A document with a wire version pushes forward.
        builder.reset();
        builder.add("maxWireVersion", 2);
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.forWireVersion(2)));

        // A document with a wire version pushes forward but not back.
        builder.reset();
        builder.add("maxWireVersion", 1);
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.forWireVersion(2)));

        // A document with a wire version clears unknown.
        final Server server2 = new Server(new InetSocketAddress("foo", 27017));
        assertThat(server2.getVersion(), is(Version.UNKNOWN));

        builder.reset();
        builder.add("maxWireVersion", 1);
        server2.update(builder.build());
        assertThat(server2.getVersion(), is(Version.forWireVersion(1)));
    }
}
