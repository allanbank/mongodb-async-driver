/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * MongoClientImplTest provides tests for the {@link MongoClientImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientImplTest {

    /** The address for the test. */
    private String myAddress = null;

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The instance under test. */
    private MongoClientImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(Client.class);

        myTestInstance = new MongoClientImpl(myMockClient);
        myAddress = "localhost:21017";
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockClient = null;

        myTestInstance = null;
        myAddress = null;
    }

    /**
     * Test method for {@link MongoClientImpl#asSerializedClient()} .
     */
    @Test
    public void testAsSerializedClient() {
        final MongoClientImpl impl = new MongoClientImpl(
                new MongoClientConfiguration());
        assertThat(impl.getClient(), instanceOf(ClientImpl.class));
        impl.close();

        final MongoClient serial = impl.asSerializedClient();
        assertThat(serial, instanceOf(MongoClientImpl.class));
        final MongoClientImpl serialImpl = (MongoClientImpl) serial;
        assertThat(serialImpl.getClient(), instanceOf(SerialClientImpl.class));
    }

    /**
     * Test method for {@link MongoClientImpl#close()}.
     */
    @Test
    public void testClose() {

        myMockClient.close();
        expectLastCall();

        replay();

        myTestInstance.close();

        verify();
    }

    /**
     * Test method for
     * {@link MongoClientImpl#MongoClientImpl(MongoClientConfiguration)} .
     */
    @Test
    public void testConstructor() {
        final MongoClientImpl impl = new MongoClientImpl(
                new MongoClientConfiguration());
        assertTrue(impl.getClient() instanceof ClientImpl);
        impl.close();
    }

    /**
     * Test method for {@link MongoClientImpl#getDatabase(java.lang.String)} .
     */
    @Test
    public void testGetDatabase() {
        final MongoDatabase database = myTestInstance.getDatabase("foo");
        assertTrue(database instanceof MongoDatabaseImpl);
        assertSame(myMockClient, ((MongoDatabaseImpl) database).myClient);
        assertEquals("foo", database.getName());
    }

    /**
     * Test method for {@link MongoClientImpl#listDatabases()}.
     */
    @Test
    public void testListDatabases() {
        final DocumentBuilder reply = BuilderFactory.start();
        final ArrayBuilder dbEntry = reply.pushArray("databases");
        dbEntry.push().addString("name", "db_1");
        dbEntry.push().addString("name", "db_2");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("listDatabases", 1);

        final Command message = new Command("admin", commandDoc.build());

        expect(myMockClient.send(eq(message), callback(reply(reply.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Arrays.asList("db_1", "db_2"),
                myTestInstance.listDatabases());

        verify();
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockClient} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockClient);
    }

    /**
     * Creates a reply around the document.
     * 
     * @param replyDoc
     *            The document to include in the reply.
     * @return The {@link Reply}
     */
    private Reply reply(final Document... replyDoc) {
        return new Reply(1, 0, 0, Arrays.asList(replyDoc), false, false, false,
                false);
    }

    /**
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockClient} object.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockClient);
    }
}
