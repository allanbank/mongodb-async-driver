/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.beans.PropertyChangeListener;
import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * ClientImplTest provides tests for the {@link ClientImpl} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class SerialClientImplTest {

    /** The address for the test. */
    private String myAddress = null;

    /** The instance under test. */
    private ClientImpl myClient;

    /** The active configuration. */
    private MongoClientConfiguration myConfig;

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

        myConfig = new MongoClientConfiguration();
        myClient = new ClientImpl(myConfig, myMockConnectionFactory);
        myTestInstance = new SerialClientImpl(myClient);
        myAddress = "localhost:27017";
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
        myAddress = null;
    }

    /**
     * Test method for {@link SerialClientImpl#close()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testClose() throws IOException {

        final Command message = new Command("testDb", BuilderFactory.start()
                .build());

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, null);
        myTestInstance.close();

        verify(mockConnection);
    }

    /**
     * Test method for {@link SerialClientImpl#getClusterType()}.
     */
    @Test
    public void testGetClusterType() {

        expect(myMockConnectionFactory.getClusterType()).andReturn(
                ClusterType.STAND_ALONE);

        replay();

        assertEquals(ClusterType.STAND_ALONE, myTestInstance.getClusterType());

        verify();
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
     * Test method for {@link SerialClientImpl#getDefaultReadPreference()}.
     */
    @Test
    public void testGetDefaultReadPreference() {

        myConfig.setDefaultReadPreference(ReadPreference.SECONDARY);

        replay();

        assertEquals(ReadPreference.SECONDARY,
                myTestInstance.getDefaultReadPreference());

        verify();
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRestartDocumentAssignable() throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        final GetMore message = new GetMore("a", "b", 123456, 23,
                ReadPreference.server("server"));
        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(eq(message), anyObject(Callback.class)))
                .andReturn(ServerNameUtils.normalize("foo"));

        replay(mockConnection);

        final MongoIterator<Document> iter = myTestInstance.restart(b);

        verify(mockConnection);

        assertThat(iter, instanceOf(MongoIteratorImpl.class));
        final MongoIteratorImpl iterImpl = (MongoIteratorImpl) iter;
        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("a"));
        assertThat(iterImpl.getCollectionName(), is("b"));
        assertThat(iterImpl.getClient(), is((Client) myClient));
        assertThat(iterImpl.getReadPerference(),
                is(ReadPreference.server("server")));
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testRestartDocumentAssignableNonCursorDoc() throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        replay();

        // Missing fields.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.LIMIT_FIELD, 23);

        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.SERVER_FIELD, "server");

        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 23);

        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");

        // Too few fields.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        // Wrong Field type.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add(MongoCursorControl.LIMIT_FIELD, "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add(MongoCursorControl.LIMIT_FIELD, 23);

        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add(MongoCursorControl.SERVER_FIELD, 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add(MongoCursorControl.SERVER_FIELD, "server");

        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add(MongoCursorControl.CURSOR_ID_FIELD, "s");
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 23);

        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add(MongoCursorControl.NAME_SPACE_FIELD, 1);
        try {
            myTestInstance.restart(b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");

        verify();

    }

    /**
     * Test method for
     * {@link ClientImpl#restart(StreamCallback, DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRestartStreamCallbackDocumentAssignable()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        final GetMore message = new GetMore("a", "b", 123456, 23,
                ReadPreference.server("server"));
        final StreamCallback<Document> mockStreamCallback = createMock(StreamCallback.class);
        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(eq(message), anyObject(Callback.class)))
                .andReturn("foo");

        replay(mockConnection, mockStreamCallback);

        final MongoCursorControl iter = myTestInstance.restart(
                mockStreamCallback, b);

        verify(mockConnection, mockStreamCallback);

        assertThat(iter, instanceOf(QueryStreamingCallback.class));
        final QueryStreamingCallback iterImpl = (QueryStreamingCallback) iter;
        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("a"));
        assertThat(iterImpl.getCollectionName(), is("b"));
        assertThat(iterImpl.getClient(), is((Client) myClient));
        assertThat(iterImpl.getAddress(), is("server"));
    }

    /**
     * Test method for {@link ClientImpl#restart(DocumentAssignable)}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testRestartStreamCallbackDocumentAssignableNonCursorDoc()
            throws IOException {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        final StreamCallback<Document> mockStreamCallback = createMock(StreamCallback.class);

        replay(mockStreamCallback);

        // Missing fields.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.LIMIT_FIELD, 23);

        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.SERVER_FIELD, "server");

        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 23);

        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add("c", 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove("c");
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");

        // Too few fields.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        // Wrong Field type.
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.BATCH_SIZE_FIELD);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);

        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add(MongoCursorControl.LIMIT_FIELD, "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.LIMIT_FIELD);
        b.add(MongoCursorControl.LIMIT_FIELD, 23);

        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add(MongoCursorControl.SERVER_FIELD, 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.SERVER_FIELD);
        b.add(MongoCursorControl.SERVER_FIELD, "server");

        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add(MongoCursorControl.CURSOR_ID_FIELD, "s");
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.CURSOR_ID_FIELD);
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 23);

        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add(MongoCursorControl.NAME_SPACE_FIELD, 1);
        try {
            myTestInstance.restart(mockStreamCallback, b);
        }
        catch (final IllegalArgumentException good) { // Good.
        }
        b.remove(MongoCursorControl.NAME_SPACE_FIELD);
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");

        verify(mockStreamCallback);

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
                12345, ReadPreference.PRIMARY);

        final Connection mockConnection = createMock(Connection.class);

        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, callback)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, callback);

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

        expect(mockConnection.send(message, null)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, null);

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
            myTestInstance.send(message, null);
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
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        expect(mockConnection.isOpen()).andReturn(false);
        expect(mockConnection.isOpen()).andReturn(false);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection);
        mockConnection
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();

        expect(mockConnection.send(message, null)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

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
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        expect(mockConnection.isOpen()).andReturn(false);
        expect(mockConnection.isOpen()).andReturn(false);
        expect(myMockConnectionFactory.connect()).andReturn(mockConnection2);
        mockConnection2
                .addPropertyChangeListener(anyObject(PropertyChangeListener.class));
        expectLastCall();
        expect(mockConnection2.send(message, null)).andReturn(myAddress);

        replay(mockConnection, mockConnection2);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

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

        expect(mockConnection.send(message, lastError, callback)).andReturn(
                myAddress);

        replay(mockConnection);

        myTestInstance.send(message, lastError, callback);

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

        expect(mockConnection.send(message, null)).andReturn(myAddress);
        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

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
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        // First pass for idle.
        expect(mockConnection.isOpen()).andReturn(true);
        expect(mockConnection.send(message, null)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, null);
        myTestInstance.send(message, null);
        myTestInstance.send(message, null);

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

        expect(mockConnection.send(message, callback)).andReturn(myAddress);

        replay(mockConnection);

        myTestInstance.send(message, callback);

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
