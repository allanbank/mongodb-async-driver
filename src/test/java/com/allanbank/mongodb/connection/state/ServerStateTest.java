/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * ServerStateTest provides tests for the {@link ServerState} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerStateTest {

    /**
     * Test method for {@link ServerState#getAverageLatency()}.
     */
    @Test
    public void testGetAverageLatency() {

        final ServerState state = new ServerState("foo");

        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.001);
        state.updateAverageLatency(10);
        assertEquals(10.0, state.getAverageLatency(), 0.1);
        for (int i = 0; i < ServerState.DECAY_SAMPLES; ++i) {
            state.updateAverageLatency(15);
        }
        assertEquals(15, state.getAverageLatency(), 1.0);

        state.setAverageLatency(100.0);
        assertEquals(100.0, state.getAverageLatency(), 0.1);
    }

    /**
     * Test method for {@link ServerState#getTags()}.
     */
    @Test
    public void testGetSetTags() {
        final ServerState state = new ServerState("foo:27017");

        assertNull(state.getTags());

        state.setTags(BuilderFactory.start().addInteger("f", 1).build());
        assertEquals(BuilderFactory.start().addInteger("f", 1).build(),
                state.getTags());

        state.setTags(null);
        assertNull(state.getTags());
    }

    /**
     * Test method for {@link ServerState#isWritable()}.
     */
    @Test
    public void testIsWritable() {
        final ServerState state = new ServerState("foo");

        assertFalse(state.isWritable());

        state.setWritable(true);

        assertTrue(state.isWritable());
    }

    /**
     * Test method for {@link ServerState#ServerState(String)}.
     */
    @Test
    public void testServerStateInvalidPort() {
        final ServerState state = new ServerState("foo:bar");
        final InetSocketAddress addr = state.getServer();
        assertEquals(ServerState.DEFAULT_PORT, addr.getPort());
        assertEquals("foo:bar", addr.getHostName());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.001);
        assertFalse(state.isWritable());
    }

    /**
     * Test method for {@link ServerState#ServerState(String)}.
     */
    @Test
    public void testServerStateMultipleColons() {
        final ServerState state = new ServerState("foo:bar:1234");
        final InetSocketAddress addr = state.getServer();
        assertEquals(1234, addr.getPort());
        assertEquals("foo:bar", addr.getHostName());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.001);
        assertFalse(state.isWritable());
    }

    /**
     * Test method for {@link ServerState#ServerState(String)}.
     */
    @Test
    public void testServerStateNoPort() {
        final ServerState state = new ServerState("foo");
        final InetSocketAddress addr = state.getServer();
        assertEquals(ServerState.DEFAULT_PORT, addr.getPort());
        assertEquals("foo", addr.getHostName());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.001);
        assertFalse(state.isWritable());
    }

}
