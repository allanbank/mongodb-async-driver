/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * MongoDatabaseImplTest provides tests for the {@link MongoDatabaseImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class MongoDatabaseImplTest {

    /** The address for the test. */
    private String myAddress = null;

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The instance under test. */
    private MongoDatabaseImpl myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(Client.class);

        myTestInstance = new MongoDatabaseImpl(myMockClient, "test");
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
     * Test method for {@link MongoDatabaseImpl#drop()}.
     */
    @Test
    public void testDrop() {
        final Document goodResult = BuilderFactory.start().addDouble("ok", 1.0)
                .build();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .build();
        final Document missingOkResult = BuilderFactory.start().build();

        final Command command = new Command("test", BuilderFactory.start()
                .addInteger("dropDatabase", 1).build());

        expect(myMockClient.send(callback(reply(goodResult)), eq(command)))
                .andReturn(myAddress);
        expect(myMockClient.send(callback(reply(badResult)), eq(command)))
                .andReturn(myAddress);
        expect(myMockClient.send(callback(reply(missingOkResult)), eq(command)))
                .andReturn(myAddress);

        replay();

        assertTrue(myTestInstance.drop());
        assertFalse(myTestInstance.drop());
        assertFalse(myTestInstance.drop());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#getCollection(String)}.
     */
    @Test
    public void testGetCollection() {
        final MongoCollection collection = myTestInstance.getCollection("foo");
        assertTrue(collection instanceof MongoCollectionImpl);
        assertSame(myTestInstance,
                ((AbstractMongoCollection) collection).myDatabase);
        assertSame(myMockClient,
                ((AbstractMongoCollection) collection).myClient);
        assertEquals("foo", collection.getName());
    }

    /**
     * Test method for {@link MongoDatabaseImpl#listCollections()}.
     */
    @Test
    public void testListCollections() {

        final Document result1 = BuilderFactory.start()
                .addString("name", "test.collection").build();
        final Document result2 = BuilderFactory.start()
                .addString("name", "test.1.oplog.$").build();

        final Query query = new Query("test", "system.namespaces",
                BuilderFactory.start().build(), null, 0, 0, 0, false,
                ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockClient.send(callback(reply(result1, result2)), eq(query)))
                .andReturn(myAddress);

        replay();

        assertEquals(Arrays.asList("collection", "1.oplog.$"),
                myTestInstance.listCollections());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runAdminCommand(String)}.
     */
    @Test
    public void testRunAdminCommandString() {
        myTestInstance = new MongoDatabaseImpl(myMockClient, "admin");

        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("admin", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply, myTestInstance.runAdminCommand("command"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runAdminCommand(String, DocumentAssignable)}.
     */
    @Test
    public void testRunAdminCommandStringDocument() {
        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("admin", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);
        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply,
                myTestInstance.runAdminCommand("command", options.build()));
        assertSame(reply,
                myTestInstance.runAdminCommand("command", options.build()));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runAdminCommand(String, String, DocumentAssignable)}
     * .
     */
    @Test
    public void testRunAdminCommandStringStringDocument() {
        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");

        final Command message = new Command("admin", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply,
                myTestInstance.runAdminCommand("command", "name", null));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String)}.
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentString() {
        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(anyObject(ReplyCallback.class), eq(message)))
                .andReturn(myAddress);

        replay(mockCallback);

        myTestInstance.runCommandAsync(mockCallback, "command");

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String, DocumentAssignable)}
     * .
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentStringDocument() {

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);
        options.addString("command", "1");

        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(anyObject(ReplyCallback.class), eq(message)))
                .andReturn(myAddress);

        replay(mockCallback);

        myTestInstance
                .runCommandAsync(mockCallback, "command", options.build());

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(Callback, String, String, DocumentAssignable)}
     * .
     */
    @Test
    public void testRunCommandAsyncCallbackOfDocumentStringStringDocument() {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);
        options.addInteger("command", 1);

        final Callback<Document> mockCallback = createMock(Callback.class);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(anyObject(ReplyCallback.class), eq(message)))
                .andReturn(myAddress);

        replay(mockCallback);

        myTestInstance.runCommandAsync(mockCallback, "command", "name",
                options.build());

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runCommandAsync(String)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncString() throws Exception {

        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply, myTestInstance.runCommandAsync("command").get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(String, DocumentAssignable)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncStringDocument() throws Exception {
        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply,
                myTestInstance.runCommandAsync("command", options.build())
                        .get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommandAsync(String, String, DocumentAssignable)}
     * .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testRunCommandAsyncStringStringDocument() throws Exception {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(
                reply,
                myTestInstance.runCommandAsync("command", "name",
                        options.build()).get());

        verify();
    }

    /**
     * Test method for {@link MongoDatabaseImpl#runCommand(String)}.
     */
    @Test
    public void testRunCommandString() {
        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply, myTestInstance.runCommand("command"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommand(String, DocumentAssignable)}.
     */
    @Test
    public void testRunCommandStringDocument() {
        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addInteger("command", 1);
        commandDoc.addBoolean("option1", true);

        final Command message = new Command(myTestInstance.getName(),
                commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply, myTestInstance.runCommand("command", options.build()));

        verify();
    }

    /**
     * Test method for
     * {@link MongoDatabaseImpl#runCommand(String, String, DocumentAssignable)}.
     */
    @Test
    public void testRunCommandStringStringDocument() {
        final DocumentBuilder options = BuilderFactory.start();
        options.addBoolean("option1", true);

        final Document reply = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.addString("command", "name");
        commandDoc.addBoolean("option1", true);

        final Command message = new Command("test", commandDoc.build());

        expect(myMockClient.send(callback(reply(reply)), eq(message)))
                .andReturn(myAddress);

        replay();

        assertSame(reply,
                myTestInstance.runCommand("command", "name", options.build()));

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
