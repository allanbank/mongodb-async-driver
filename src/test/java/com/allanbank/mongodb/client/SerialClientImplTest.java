/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeListener;
import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;

/**
 * ClientImplTest provides tests for the {@link ClientImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class SerialClientImplTest {

    /** The instance under test. */
    private ClientImpl myClient;

    /** The active configuration. */
    private MongoDbConfiguration myConfig;

    /** A mock connection factory. */
    private ConnectionFactory myMockConnectionFactory;

    /** The instance under test. */
    private SerialClientImpl myTestInstance;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockConnectionFactory = EasyMock.createMock(ConnectionFactory.class);

        myConfig = new MongoDbConfiguration();
        myClient = new ClientImpl(myConfig, myMockConnectionFactory);
        myTestInstance = new SerialClientImpl(myClient);
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockConnectionFactory = null;

        myConfig = null;
        myClient = null;
        myTestInstance = null;
    }

    /**
     * Test method for {@link SerialClientImpl#close()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testClose() throws IOException {

        final Command message = new Command("testDb", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(null, message);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.getPendingCount()).andReturn(0);

        mockConnection.send(null, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(message);
        myTestInstance.close();
        myTestInstance.send(message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#getConfig()}.
     */
    @Test
    public void testGetConfig() {
        assertSame(myConfig, myTestInstance.getConfig());
    }

    /**
     * Test method for {@link SerialClientImpl#getDefaultDurability()}.
     */
    @Test
    public void testGetDefaultDurability() {
        assertSame(myConfig.getDefaultDurability(),
                myTestInstance.getDefaultDurability());
        myConfig.setDefaultDurability(Durability.journalDurable(1000));
        assertSame(myConfig.getDefaultDurability(),
                myTestInstance.getDefaultDurability());
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendGetMoreCallbackOfReply() throws IOException {

        final Callback<Reply> callback = createMock(Callback.class);
        final GetMore message = new GetMore("testDb", "collection", 1234L,
                12345);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(callback, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(callback, message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessage() throws IOException {
        final Update message = new Update("testDb", "collection", null, null,
                false, false);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(null, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessageAndCreateConnectionFailes() throws IOException {
        final Update message = new Update("testDb", "collection", null, null,
                false, false);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andThrow(new IOException());

        replay(mockConnection);

        try {
            myTestInstance.send(message);
            fail("Should have thrown a MongoDbException.");
        }
        catch (final MongoDbException good) {
            // good.
        }

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageClosedExisting() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.send(null, message);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(false);
        expect(mockConnection.isOpen()).andReturn(false);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(null, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(message);
        myTestInstance.send(message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessageCreatesSecondConnectionOnClosed()
            throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);
        final Connection mockConnection2 = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.send(null, message);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(false);
        expect(mockConnection.isOpen()).andReturn(false);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection2.send(null, message);
        expectLastCall();

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message);
        myTestInstance.send(message);

        verify(mockConnection, mockConnection2);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendMessageGetLastErrorCallbackOfReply() throws IOException {
        final Message message = new Update("testDb", "collection", null, null,
                false, false);
        final GetLastError lastError = new GetLastError("testDb", false, false,
                0, 0);
        final Callback<Reply> callback = createMock(Callback.class);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(callback, message, lastError);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(callback, message, lastError);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessagePicksIdleExisting() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(null, message);
        expectLastCall();
        expect(mockConnection.isOpen()).andReturn(true);
        mockConnection.send(null, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(message);
        myTestInstance.send(message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testSendMessagePicksMostIdleWhenAllPending() throws IOException {
        final Message message = new Command("db", BuilderFactory.start()
                .build());

        myConfig.setMaxConnectionCount(2);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        mockConnection.send(null, message);
        expectLastCall();

        expect(mockConnection.isOpen()).andReturn(true);
        mockConnection.send(null, message);
        expectLastCall();

        // First pass for idle.
        expect(mockConnection.isOpen()).andReturn(true);
        mockConnection.send(null, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(message);
        myTestInstance.send(message);
        myTestInstance.send(message);

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#send} .
     * 
     * @throws IOException
     *             On a failure setting up the test.
     */
    @Test
    public void testSendQueryCallbackOfReply() throws IOException {
        final Query message = new Query("db", "coll", null, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);
        final Callback<Reply> callback = createMock(Callback.class);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        mockConnection.send(callback, message);
        expectLastCall();

        replay(mockConnection);

        myTestInstance.send(callback, message);

        verify(mockConnection);
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockConnectionFactory);
    }

    /**
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockConnectionFactory} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockConnectionFactory);
    }
}
