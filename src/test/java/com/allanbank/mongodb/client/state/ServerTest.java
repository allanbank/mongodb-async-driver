/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
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

        long total = 0;
        final Server server = new Server(new InetSocketAddress("foo", 27017));

        assertEquals(Double.MAX_VALUE, server.getAverageLatency(), 0.001);
        server.updateAverageLatency(10000000);
        total += 10000000;
        assertEquals(10.0, server.getAverageLatency(), 0.1);
        for (int i = 0; i < Server.DECAY_SAMPLES; ++i) {
            server.updateAverageLatency(15000000);
            total += 15000000;
        }
        assertEquals(15, server.getAverageLatency(), 1.0);

        assertThat(server.getTotalLatencyNanoSeconds(), is(total));
    }

    /**
     * Test method for {@link Server#getMessagesSent()}.
     */
    @Test
    public void testGetMessagesSent() {
        final Server server = new Server(new InetSocketAddress("foo", 27017));

        assertThat(server.getMessagesSent(), is(0L));
        server.incrementMessagesSent();
        assertThat(server.getMessagesSent(), is(1L));
        server.incrementMessagesSent();
        assertThat(server.getMessagesSent(), is(2L));
    }

    /**
     * Test method for {@link Server#getRepliesReceived()}.
     */
    @Test
    public void testGetRepliesReceived() {
        final Server server = new Server(new InetSocketAddress("foo", 27017));

        assertThat(server.getRepliesReceived(), is(0L));
        server.incrementRepliesReceived();
        assertThat(server.getRepliesReceived(), is(1L));
        server.incrementRepliesReceived();
        assertThat(server.getRepliesReceived(), is(2L));
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

        // Clear the tags.
        builder.reset();
        builder.pushArray("versionArray");
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("1.2.3")));

        // Set them again.
        builder.reset();
        builder.pushArray("versionArray").add(2).add(3L).add(4.0);
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("2.3.4")));

        // A document without tags has no effect (still set).
        builder.reset();
        server.update(builder.build());
        assertThat(server.getVersion(), is(Version.parse("2.3.4")));
    }
}
