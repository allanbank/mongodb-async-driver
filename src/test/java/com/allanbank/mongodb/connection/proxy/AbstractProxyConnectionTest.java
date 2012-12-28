/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.proxy;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.PendingMessage;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AbstractProxyConnectionTest provides tests for the
 * {@link AbstractProxyConnection} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AbstractProxyConnectionTest {
    /** An empty document for use in constructing messages. */
    public static final Document EMPTY_DOC = BuilderFactory.start().build();

    /**
     * Test method for
     * {@link AbstractProxyConnection#addPropertyChangeListener(PropertyChangeListener)}
     * .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAbstractProxyConnectionProxiedChangeListenerEquals()
            throws IOException {
        final PropertyChangeListener mockListener = createMock(PropertyChangeListener.class);

        final Connection mockConnetion = createMock(Connection.class);
        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        final PropertyChangeListener proxiedListener = new AbstractProxyConnection.ProxiedChangeListener(
                conn, mockListener);

        replay(mockConnetion, mockListener);

        assertEquals(proxiedListener,
                new AbstractProxyConnection.ProxiedChangeListener(conn,
                        mockListener));
        assertEquals(proxiedListener.hashCode(),
                new AbstractProxyConnection.ProxiedChangeListener(conn,
                        mockListener).hashCode());
        assertFalse(proxiedListener.equals("false"));
        assertFalse(proxiedListener.equals(null));
        assertTrue(proxiedListener.equals(proxiedListener));
        assertEquals(13, new AbstractProxyConnection.ProxiedChangeListener(
                conn, null).hashCode());

        verify(mockConnetion, mockListener);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#addPropertyChangeListener(PropertyChangeListener)}
     * .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAbstractProxyConnectionProxiedChangeListenerPropertyChanges()
            throws IOException {
        final PropertyChangeListener mockListener = createMock(PropertyChangeListener.class);

        final Connection mockConnetion = createMock(Connection.class);
        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        final PropertyChangeListener proxiedListener = new AbstractProxyConnection.ProxiedChangeListener(
                conn, mockListener);

        final Capture<PropertyChangeEvent> captured = new Capture<PropertyChangeEvent>();
        mockListener.propertyChange(capture(captured));

        replay(mockConnetion, mockListener);

        final PropertyChangeEvent event = new PropertyChangeEvent("foo",
                "prop", "old", "new");
        proxiedListener.propertyChange(event);

        verify(mockConnetion, mockListener);

        final PropertyChangeEvent newEvent = captured.getValue();
        assertSame(conn, newEvent.getSource());
        assertSame(event.getPropertyName(), newEvent.getPropertyName());
        assertSame(event.getOldValue(), newEvent.getOldValue());
        assertSame(event.getNewValue(), newEvent.getNewValue());
    }

    /**
     * Test method for {@link AbstractProxyConnection#addPending(List)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     * @throws InterruptedException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAddPending() throws IOException, InterruptedException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final List<PendingMessage> pendings = Collections
                .singletonList(new PendingMessage(1, msg));

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.addPending(pendings);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.addPending(pendings);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#addPending(List)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     * @throws InterruptedException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAddPendingOnThrow() throws IOException,
            InterruptedException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final MongoDbException thrown = new MongoDbException();
        final List<PendingMessage> pendings = Collections
                .singletonList(new PendingMessage(1, msg));

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.addPending(pendings);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.addPending(pendings);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#addPropertyChangeListener(PropertyChangeListener)}
     * .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAddPropertyChangeListener() throws IOException {
        final PropertyChangeListener mockListener = createMock(PropertyChangeListener.class);

        final Connection mockConnetion = createMock(Connection.class);
        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        final PropertyChangeListener proxiedListener = new AbstractProxyConnection.ProxiedChangeListener(
                conn, mockListener);

        // Message.
        mockConnetion.addPropertyChangeListener(proxiedListener);
        expectLastCall();

        mockConnetion.removePropertyChangeListener(proxiedListener);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion, mockListener);

        conn.addPropertyChangeListener(mockListener);
        conn.removePropertyChangeListener(mockListener);
        IOUtils.close(conn);

        verify(mockConnetion, mockListener);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#addPropertyChangeListener(PropertyChangeListener)}
     * .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAddPropertyChangeListenerOnThrows() throws IOException {
        final PropertyChangeListener mockListener = createMock(PropertyChangeListener.class);
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);
        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        final PropertyChangeListener proxiedListener = new AbstractProxyConnection.ProxiedChangeListener(
                conn, mockListener);

        // Message.
        mockConnetion.addPropertyChangeListener(proxiedListener);
        expectLastCall().andThrow(thrown);

        mockListener.propertyChange(anyObject(PropertyChangeEvent.class));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion, mockListener);

        try {
            conn.addPropertyChangeListener(mockListener);
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        verify(mockConnetion, mockListener);
    }

    /**
     * Test method for {@link AbstractProxyConnection#close()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testClose() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.close();

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#drainPending(List)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testDrainPending() throws IOException {
        final List<PendingMessage> pendings = Collections.emptyList();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.drainPending(pendings);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.drainPending(pendings);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#drainPending(List)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testDrainPendingOnThrow() throws IOException {

        final MongoDbException thrown = new MongoDbException();
        final List<PendingMessage> pendings = Collections.emptyList();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.drainPending(pendings);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.drainPending(pendings);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);

    }

    /**
     * Test method for {@link AbstractProxyConnection#flush()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testFlush() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.flush();
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.flush();

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#flush()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testFlushOnThrow() throws IOException {
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.flush();
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.flush();
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);

    }

    /**
     * Test method for {@link AbstractProxyConnection#getPendingCount()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetPendingCount() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.getPendingCount()).andReturn(1);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        assertEquals(1, conn.getPendingCount());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#getPendingCount()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetPendingCountOnThrow() throws IOException {
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.getPendingCount()).andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.getPendingCount();
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#getProxiedConnection()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testGetProxiedConnection() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        assertSame(mockConnetion, conn.getProxiedConnection());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isIdle()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsIdle() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isIdle()).andReturn(false);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        assertEquals(false, conn.isIdle());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isIdle()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsIdleOnThrow() throws IOException {
        final MongoDbException thrown = new MongoDbException();
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isIdle()).andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.isIdle();
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isOpen()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsOpen() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isOpen()).andReturn(true);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        assertEquals(true, conn.isOpen());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isOpen()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsOpenOnError() throws IOException {
        final MongoDbException thrown = new MongoDbException();
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isOpen()).andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.isOpen();
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#raiseErrors(MongoDbException, boolean)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testRaiseErrors() throws IOException {
        final MongoDbException thrown = new MongoDbException();
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.raiseErrors(thrown, false);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.raiseErrors(thrown, false);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArray() throws IOException {
        final String address = "localhost:27017";

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final FutureCallback<Reply> callback = new FutureCallback<Reply>();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.send(msg, callback)).andReturn(address);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.send(msg, callback);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#send(Message, Callback)} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArrayOnThrow() throws IOException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final FutureCallback<Reply> callback = new FutureCallback<Reply>();
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, callback);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.send(msg, callback);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }
        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#send}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendMessageArray() throws IOException {
        final String address = "localhost:27017";

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.send(msg, null)).andReturn(address);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#send}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendMessageArrayOnThrow() throws IOException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.send(msg, null);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#shutdown()} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testShutdown() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.shutdown();
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.shutdown();

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#waitForClosed(int, TimeUnit)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testWaitForClosed() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.waitForClosed(1, TimeUnit.DAYS);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        conn.waitForClosed(1, TimeUnit.DAYS);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#waitForClosed(int, TimeUnit)}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testWaitForClosedOnThrow() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);
        final MongoDbException thrown = new MongoDbException();

        // Message.
        mockConnetion.waitForClosed(1, TimeUnit.DAYS);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion, new MongoClientConfiguration());

        try {
            conn.waitForClosed(1, TimeUnit.DAYS);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * TestProxiedConnection provides a connection for testing.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static final class TestProxiedConnection extends
            AbstractProxyConnection {

        /**
         * Creates a new TestProxiedConnection.
         * 
         * @param proxiedConnection
         *            The connection to forward to.
         * @param config
         *            The MongoDB client configuration.
         */
        public TestProxiedConnection(final Connection proxiedConnection,
                final MongoClientConfiguration config) {
            super(proxiedConnection, config);
        }
    }
}
