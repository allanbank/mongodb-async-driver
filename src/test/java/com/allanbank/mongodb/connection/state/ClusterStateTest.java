/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collections;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

/**
 * ClusterStateTest provides tests for the {@link ClusterState}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterStateTest {

    /**
     * Test method for {@link ClusterState#add(String)}.
     */
    @Test
    public void testAdd() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        state.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, state.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);
        state.addListener(mockListener);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        assertSame(ss, state.add("foo"));

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("server", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertNull(event.getValue().getOldValue());
        assertSame(ss, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#getNonWritableServers()}.
     */
    @Test
    public void testGetNonWritableServers() {
        final ClusterState state = new ClusterState();

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markWritable(ss);
        assertTrue(ss.isWritable());

        assertEquals(Collections.singletonList(ss), state.getServers());
        assertEquals(Collections.singletonList(ss), state.getWritableServers());
        assertEquals(0, state.getNonWritableServers().size());

        state.markNotWritable(ss);
        assertFalse(ss.isWritable());

        assertEquals(Collections.singletonList(ss), state.getServers());
        assertEquals(Collections.singletonList(ss),
                state.getNonWritableServers());
        assertEquals(0, state.getWritableServers().size());

    }

    /**
     * Test method for {@link ClusterState#markNotWritable(ServerState)}.
     */
    @Test
    public void testMarkNotWritable() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markWritable(ss);

        assertTrue(ss.isWritable());

        state.addListener(mockListener);

        state.markNotWritable(ss);
        assertFalse(ss.isWritable());

        state.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.TRUE, event.getValue().getOldValue());
        assertEquals(Boolean.FALSE, event.getValue().getNewValue());
    }

    /**
     * Test method for {@link ClusterState#markWritable(ServerState)}.
     */
    @Test
    public void testMarkWritable() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markNotWritable(ss);

        assertFalse(ss.isWritable());

        state.addListener(mockListener);

        state.markWritable(ss);
        assertTrue(ss.isWritable());
        state.markWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

    /**
     * Test method for
     * {@link ClusterState#removeListener(PropertyChangeListener)}.
     */
    @Test
    public void testRemoveListener() {
        final ClusterState state = new ClusterState();

        final PropertyChangeListener mockListener = EasyMock
                .createMock(PropertyChangeListener.class);

        // Should only get notified of the new server once.
        final Capture<PropertyChangeEvent> event = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(EasyMock.capture(event));
        expectLastCall();

        replay(mockListener);

        final ServerState ss = state.add("foo");
        assertEquals("foo", ss.getServer().getHostName());
        assertEquals(ServerState.DEFAULT_PORT, ss.getServer().getPort());

        state.markNotWritable(ss);

        assertFalse(ss.isWritable());

        state.addListener(mockListener);

        state.markWritable(ss);
        assertTrue(ss.isWritable());
        state.markWritable(ss);

        state.removeListener(mockListener);

        state.markNotWritable(ss);

        verify(mockListener);

        assertTrue(event.hasCaptured());
        assertEquals("writable", event.getValue().getPropertyName());
        assertSame(state, event.getValue().getSource());
        assertEquals(Boolean.FALSE, event.getValue().getOldValue());
        assertEquals(Boolean.TRUE, event.getValue().getNewValue());
    }

}
