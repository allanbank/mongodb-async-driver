/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.captureDouble;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * SecondsBehindCallbackTest provides tests for the
 * {@link SecondsBehindCallback} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SecondsBehindCallbackTest {

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallback() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 1);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "foo").add("optimeDate", optime);
        membersBuilder.push().add("name", "bar")
                .add("optimeDate", new Date(optime.getTime() - 1000));

        final Capture<Double> update = new Capture<Double>();

        final ServerState state = createMock(ServerState.class);
        expect(state.getName()).andReturn("foo");
        state.setSecondsBehind(captureDouble(update));
        expectLastCall();

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);

        assertEquals(0.0, update.getValue().doubleValue(), 0.001);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackNoServerState() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 1);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "foo").add("optimeDate", optime);
        membersBuilder.push().add("name", "bar")
                .add("optimeDate", new Date(optime.getTime() - 1000));

        final ServerState state = null;

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackBehind() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 1);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "foo")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final Capture<Double> update = new Capture<Double>();

        final ServerState state = createMock(ServerState.class);
        expect(state.getName()).andReturn("foo");
        state.setSecondsBehind(captureDouble(update));
        expectLastCall();

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);

        assertEquals(1.5, update.getValue().doubleValue(), 0.001);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackBehindNoState() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "foo")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final ServerState state = createMock(ServerState.class);
        state.setSecondsBehind(Double.MAX_VALUE);
        expectLastCall();

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackBehindNotPrimaryOrSecondary() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 3);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "foo")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final ServerState state = createMock(ServerState.class);
        state.setSecondsBehind(Double.MAX_VALUE);
        expectLastCall();

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackNoOptime() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 2);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "foo");
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "baz")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final ServerState state = createMock(ServerState.class);
        expect(state.getName()).andReturn("foo");

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackNotFound() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 1);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "baz")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final ServerState state = createMock(ServerState.class);
        expect(state.getName()).andReturn("foo");

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#callback} .
     */
    @Test
    public void testCallbackOptimeNotTime() {

        final Date optime = new Date();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("myState", 1);
        final ArrayBuilder membersBuilder = builder.pushArray("members");
        membersBuilder.push().add("name", "foo").add("optimeDate", 1);
        membersBuilder.push().add("name", "bar").add("optimeDate", optime);
        membersBuilder.push().add("name", "baz")
                .add("optimeDate", new Date(optime.getTime() - 1500));

        final ServerState state = createMock(ServerState.class);
        expect(state.getName()).andReturn("foo");

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.callback(reply(builder));

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#exception} .
     */
    @Test
    public void testException() {
        final ServerState state = createMock(ServerState.class);

        state.setSecondsBehind(Integer.MAX_VALUE);
        expectLastCall();

        replay(state);

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.exception(null);

        verify(state);
    }

    /**
     * Test method for {@link SecondsBehindCallback#exception} .
     */
    @Test
    public void testExceptionNoServerState() {
        final ServerState state = null;

        final SecondsBehindCallback callback = new SecondsBehindCallback(state);
        callback.exception(null);
    }
}
