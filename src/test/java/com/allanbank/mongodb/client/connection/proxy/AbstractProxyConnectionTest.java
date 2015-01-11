/*
 * #%L
 * AbstractProxyConnectionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection.proxy;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AbstractProxyConnectionTest provides tests for the
 * {@link AbstractProxyConnection} class.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

        conn.close();

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

        assertSame(mockConnetion, conn.getProxiedConnection());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#getServerName}.
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testGetServerName() throws IOException {

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.getServerName()).andReturn("foo");

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        assertThat(conn.getServerName(), is("foo"));

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isAvailable()} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsAvailable() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isAvailable()).andReturn(true);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        assertEquals(true, conn.isAvailable());

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#isAvailable()} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIsAvailableOnError() throws IOException {
        final MongoDbException thrown = new MongoDbException();
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        expect(mockConnetion.isAvailable()).andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        try {
            conn.isAvailable();
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
                mockConnetion);

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
     * {@link AbstractProxyConnection#raiseErrors(MongoDbException)} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testRaiseErrors() throws IOException {
        final MongoDbException thrown = new MongoDbException();
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.raiseErrors(thrown);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        conn.raiseErrors(thrown);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#send(Message, ReplyCallback)} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArray() throws IOException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final FutureReplyCallback callback = new FutureReplyCallback();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, callback);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        conn.send(msg, callback);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AbstractProxyConnection#send(Message, ReplyCallback)} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArrayOnThrow() throws IOException {
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final FutureReplyCallback callback = new FutureReplyCallback();
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, callback);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

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
        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final GetLastError msg2 = new GetLastError("db", Durability.ACK);

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, msg2, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        conn.send(msg, msg2, null);

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
        final GetLastError msg2 = new GetLastError("db", Durability.ACK);
        final MongoDbException thrown = new MongoDbException();

        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.send(msg, msg2, null);
        expectLastCall().andThrow(thrown);

        mockConnetion.close();
        expectLastCall().times(2);

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        try {
            conn.send(msg, msg2, null);
            fail("Should have thrown the exception.");
        }
        catch (final MongoDbException good) {
            assertSame(thrown, good);
        }

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AbstractProxyConnection#shutdown(boolean)} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testShutdown() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Message.
        mockConnetion.shutdown(true);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final TestProxiedConnection conn = new TestProxiedConnection(
                mockConnetion);

        conn.shutdown(true);

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
                mockConnetion);

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
                mockConnetion);

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
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static final class TestProxiedConnection
            extends AbstractProxyConnection {

        /**
         * Creates a new TestProxiedConnection.
         *
         * @param proxiedConnection
         *            The connection to forward to.
         */
        public TestProxiedConnection(final Connection proxiedConnection) {
            super(proxiedConnection);
        }
    }
}
