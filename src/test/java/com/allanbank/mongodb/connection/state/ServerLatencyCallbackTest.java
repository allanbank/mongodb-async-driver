/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.captureDouble;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.junit.Assert.assertTrue;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.bson.Document;

/**
 * ServerLatencyCallbackTest provides tests for the
 * {@link ServerLatencyCallback} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyCallbackTest {

    /**
     * Test method for {@link ServerLatencyCallback#callback} .
     */
    @Test
    public void testCallback() {
        final Capture<Double> latencyUpdate = new Capture<Double>();

        final ServerState state = createMock(ServerState.class);
        state.updateAverageLatency(captureDouble(latencyUpdate));
        expectLastCall();
        state.setTags((Document) isNull());
        expectLastCall();

        EasyMock.replay(state);

        final ServerLatencyCallback callback = new ServerLatencyCallback(state);
        callback.callback(reply());

        EasyMock.verify(state);

        assertTrue("Latency should be greater than or equal to 0: "
                + latencyUpdate.getValue(), 0L <= latencyUpdate.getValue()
                .longValue());
        assertTrue(
                "Latency should be less than 100: " + latencyUpdate.getValue(),
                latencyUpdate.getValue().longValue() < 100);
    }

    /**
     * Test method for {@link ServerLatencyCallback#callback} .
     * 
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testDelayCallback() throws InterruptedException {
        final Capture<Double> latencyUpdate = new Capture<Double>();

        final ServerState state = createMock(ServerState.class);
        state.updateAverageLatency(captureDouble(latencyUpdate));
        expectLastCall();
        state.setTags((Document) isNull());
        expectLastCall();

        EasyMock.replay(state);

        final ServerLatencyCallback callback = new ServerLatencyCallback(state);
        Thread.sleep(50);
        callback.callback(reply());

        EasyMock.verify(state);

        assertTrue("Latency should be greater than or equal to 50: "
                + latencyUpdate.getValue(), 50L <= latencyUpdate.getValue()
                .longValue());
        assertTrue(
                "Latency should be less than 150: " + latencyUpdate.getValue(),
                latencyUpdate.getValue().longValue() < 150);
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.connection.state.ServerLatencyCallback#exception(java.lang.Throwable)}
     * .
     */
    @Test
    public void testException() {
        final ServerState state = createMock(ServerState.class);

        EasyMock.replay(state);

        final ServerLatencyCallback callback = new ServerLatencyCallback(state);
        callback.exception(null);

        EasyMock.verify(state);
    }

}
