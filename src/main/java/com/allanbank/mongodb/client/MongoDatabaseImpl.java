/*
 * #%L
 * MongoDatabaseImpl.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.ListenableFuture;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ProfilingStatus;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.ListCollections;
import com.allanbank.mongodb.builder.ListCollections.Builder;
import com.allanbank.mongodb.client.callback.CursorCallback;
import com.allanbank.mongodb.client.callback.CursorStreamingCallback;
import com.allanbank.mongodb.client.callback.ReplyCommandCallback;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.ListCollectionsCommand;
import com.allanbank.mongodb.util.FutureUtils;

/**
 * Implementation of the {@link MongoDatabase} interface.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDatabaseImpl
        implements MongoDatabase {

    /** An empty query document. */
    public static final Document EMPTY_QUERY = MongoCollection.ALL;

    /** The client for interacting with MongoDB. */
    protected final Client myClient;

    /** The 'admin' database. */
    private MongoDatabase myAdminDatabase;

    /** The set of databases in use. */
    private final ConcurrentMap<String, Reference<MongoCollection>> myCollections;

    /** The {@link Durability} for writes from this database instance. */
    private Durability myDurability;

    /** The {@link MongoClient}. */
    private final MongoClient myMongoClient;

    /** The name of the database we interact with. */
    private final String myName;

    /** The {@link ReadPreference} for reads from this database instance. */
    private ReadPreference myReadPreference;

    /** The queue of references to the collections that have been reclaimed. */
    private final ReferenceQueue<MongoCollection> myReferenceQueue = new ReferenceQueue<MongoCollection>();

    /**
     * Create a new MongoDatabaseClient.
     *
     * @param mongoClient
     *            The {@link MongoClient}.
     * @param client
     *            The client for interacting with MongoDB.
     * @param name
     *            The name of the database we interact with.
     */
    public MongoDatabaseImpl(final MongoClient mongoClient,
            final Client client, final String name) {
        myMongoClient = mongoClient;
        myClient = client;
        myName = name;
        myDurability = null;
        myReadPreference = null;
        myCollections = new ConcurrentHashMap<String, Reference<MongoCollection>>();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Issues a command to create the collection with the specified name and
     * options.
     * </p>
     */
    @Override
    public boolean createCappedCollection(final String name, final long size)
            throws MongoDbException {
        return createCollection(name, BuilderFactory.start()
                .add("capped", true).add("size", size));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Issues a command to create the collection with the specified name and
     * options.
     * </p>
     */
    @Override
    public boolean createCollection(final String name,
            final DocumentAssignable options) throws MongoDbException {
        final Document result = runCommand("create", name, options);
        final NumericElement okElem = result.get(NumericElement.class, "ok");

        return ((okElem != null) && (okElem.getIntValue() > 0));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to issue a "dropDatabase" command.
     * </p>
     *
     * @see MongoDatabase#drop()
     */
    @Override
    public boolean drop() {
        final Document result = runCommand("dropDatabase");
        final NumericElement okElem = result.get(NumericElement.class, "ok");

        return ((okElem != null) && (okElem.getIntValue() > 0));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() {
        return myMongoClient.listDatabaseNames().contains(getName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a new {@link SynchronousMongoCollectionImpl}.
     * </p>
     *
     * @see MongoDatabase#getCollection(String)
     */
    @Override
    public MongoCollection getCollection(final String name) {
        MongoCollection collection = null;
        Reference<MongoCollection> ref = myCollections.get(name);
        if (ref != null) {
            collection = ref.get();
            if (collection == null) {
                // Reference was take n from the map. Remove it from the map.
                myCollections.remove(name, ref);
            }
        }

        // Create a new one.
        if (collection == null) {
            collection = new SynchronousMongoCollectionImpl(myClient, this,
                    name);
            ref = new NamedReference<MongoCollection>(name, collection,
                    myReferenceQueue);

            final Reference<MongoCollection> existing = myCollections
                    .putIfAbsent(name, ref);
            if (existing != null) {
                final MongoCollection existingCollection = existing.get();
                if (existingCollection != null) {
                    collection = existingCollection;
                }
                // Extremely unlikely but if the reference came and went that
                // quick it is the next guys problem to add one. We will return
                // the one we created.
            }
        }

        // Clean out any garbage collected references.
        Reference<?> polled;
        while ((polled = myReferenceQueue.poll()) != null) {
            if (polled instanceof NamedReference) {
                myCollections.remove(((NamedReference<?>) polled).getName(),
                        polled);
            }
        }

        return collection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Durability getDurability() {
        Durability result = myDurability;
        if (result == null) {
            result = myClient.getDefaultDurability();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return myName;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to query the system.namespace collection for the names of all
     * of the collections.
     * </p>
     *
     * @see MongoDatabase#getProfilingStatus()
     */
    @Override
    public ProfilingStatus getProfilingStatus() throws MongoDbException {
        final Document result = runCommand("profile", -1, null);

        final NumericElement level = result.get(NumericElement.class, "was");
        final NumericElement millis = result
                .get(NumericElement.class, "slowms");

        if ((level != null) && (millis != null)) {
            final ProfilingStatus.Level l = ProfilingStatus.Level
                    .fromValue(level.getIntValue());
            if (l != null) {
                switch (l) {
                case NONE:
                    return ProfilingStatus.OFF;
                case ALL:
                    return ProfilingStatus.ON;
                case SLOW_ONLY:
                    return ProfilingStatus.slow(millis.getIntValue());
                }
            }
        }

        // undefined?
        return null;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadPreference getReadPreference() {
        ReadPreference result = myReadPreference;
        if (result == null) {
            result = myClient.getDefaultReadPreference();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #listCollections(Builder)} and extract the
     * names from the returned documents.
     * </p>
     *
     * @see MongoDatabase#listCollectionNames()
     */
    @Override
    public List<String> listCollectionNames() {
        final List<String> names = new ArrayList<String>();

        final MongoIterator<Document> iter = listCollections(ListCollections
                .builder());
        try {
            while (iter.hasNext()) {
                final Document collection = iter.next();
                final StringElement nameElement = collection.findFirst(
                        StringElement.class, "name");
                if (nameElement != null) {
                    names.add(nameElement.getValue());
                }
            }
        }
        finally {
            iter.close();
        }

        return names;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #runCommand(String)} on the 'admin' database.
     * </p>
     *
     * @see #runCommandAsync(String, DocumentAssignable)
     */
    @Override
    public Document runAdminCommand(final String command)
            throws MongoDbException {
        return getAdminDatabase().runCommand(command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(String, DocumentAssignable)
     */
    @Override
    public Document runAdminCommand(final String command,
            final DocumentAssignable options) throws MongoDbException {
        return getAdminDatabase().runCommand(command, options);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, String, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(String, String, DocumentAssignable)
     */
    @Override
    public Document runAdminCommand(final String commandName,
            final String commandValue, final DocumentAssignable options)
            throws MongoDbException {
        return getAdminDatabase()
                .runCommand(commandName, commandValue, options);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the {@link #runCommandAsync(DocumentAssignable)}
     * method.
     * </p>
     *
     * @see #runCommandAsync(DocumentAssignable)
     */
    @Override
    public Document runCommand(final DocumentAssignable command)
            throws MongoDbException {
        return FutureUtils.unwrap(runCommandAsync(command));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, DocumentAssignable)} method with
     * <code>null</code> options.
     * </p>
     *
     * @see #runCommandAsync(String, DocumentAssignable)
     */
    @Override
    public Document runCommand(final String command) throws MongoDbException {
        return FutureUtils.unwrap(runCommandAsync(command, null));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(String, DocumentAssignable)
     */
    @Override
    public Document runCommand(final String command,
            final DocumentAssignable options) throws MongoDbException {
        return FutureUtils.unwrap(runCommandAsync(command, options));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, int, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(String, int, DocumentAssignable)
     */
    @Override
    public Document runCommand(final String commandName,
            final int commandValue, final DocumentAssignable options)
            throws MongoDbException {
        return FutureUtils.unwrap(runCommandAsync(commandName, commandValue,
                options));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(String, String, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(String, String, DocumentAssignable)
     */
    @Override
    public Document runCommand(final String commandName,
            final String commandValue, final DocumentAssignable options)
            throws MongoDbException {
        return FutureUtils.unwrap(runCommandAsync(commandName, commandValue,
                options));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, DocumentAssignable, Version)} method
     * with {@code null} as the version.
     * </p>
     *
     * @see #runCommandAsync(Callback, DocumentAssignable, Version)
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final DocumentAssignable command) throws MongoDbException {
        runCommandAsync(reply, command, null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to build a {@link Command} message and send it to the server.
     * </p>
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final DocumentAssignable command, final Version requireServerVersion)
            throws MongoDbException {
        final Command commandMessage = new Command(myName,
                Command.COMMAND_COLLECTION, command.asDocument(),
                ReadPreference.PRIMARY,
                VersionRange.minimum(requireServerVersion));

        myClient.send(commandMessage, new ReplyCommandCallback(reply));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, String, DocumentAssignable)} method
     * with <code>null</code> for the options.
     * </p>
     *
     * @see #runCommandAsync(Callback, String, DocumentAssignable)
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final String command) throws MongoDbException {
        runCommandAsync(reply, command, null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to build the command document and call
     * {@link #runCommandAsync(Callback, DocumentAssignable)}.
     * </p>
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final String command, final DocumentAssignable options)
            throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger(command, 1);
        addOptions(command, options, builder);

        runCommandAsync(reply, builder);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to build the command document and call
     * {@link #runCommandAsync(Callback, DocumentAssignable)}.
     * </p>
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final String commandName, final int commandValue,
            final DocumentAssignable options) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add(commandName, commandValue);
        addOptions(commandName, options, builder);

        runCommandAsync(reply, builder);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to build the command document and call
     * {@link #runCommandAsync(Callback, DocumentAssignable)}.
     * </p>
     */
    @Override
    public void runCommandAsync(final Callback<Document> reply,
            final String commandName, final String commandValue,
            final DocumentAssignable options) throws MongoDbException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add(commandName, commandValue);
        addOptions(commandName, options, builder);

        runCommandAsync(reply, builder);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(Callback, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> runCommandAsync(
            final DocumentAssignable command) throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        runCommandAsync(future, command);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #runCommandAsync(Callback, DocumentAssignable)}
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final DocumentAssignable command) throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #runCommandAsync(Callback, DocumentAssignable, Version)} with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final DocumentAssignable command,
            final Version requiredServerVersion) throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply), command,
                requiredServerVersion);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #runCommandAsync(Callback, String)} with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final String command) throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply), command);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #runCommandAsync(Callback, String, DocumentAssignable)} with an
     * adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final String command, final DocumentAssignable options)
            throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply), command,
                options);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #runCommandAsync(Callback, String, int, DocumentAssignable)} with
     * an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final String commandName, final int commandValue,
            final DocumentAssignable options) throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply),
                commandName, commandValue, options);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #runCommandAsync(Callback, String, String, DocumentAssignable)}
     * with an adapter for the {@link LambdaCallback}.
     * </p>
     */
    @Override
    public void runCommandAsync(final LambdaCallback<Document> reply,
            final String commandName, final String commandValue,
            final DocumentAssignable options) throws MongoDbException {
        runCommandAsync(new LambdaCallbackAdapter<Document>(reply),
                commandName, commandValue, options);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, String, DocumentAssignable)} method
     * with <code>null</code> options.
     * </p>
     *
     * @see #runCommandAsync(Callback, String, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> runCommandAsync(final String command)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        runCommandAsync(future, command, null);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, String, DocumentAssignable)} method.
     * </p>
     *
     * @see #runCommandAsync(Callback, String, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> runCommandAsync(final String command,
            final DocumentAssignable options) throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        runCommandAsync(future, command, options);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, String, int, DocumentAssignable)}
     * method.
     * </p>
     *
     * @see #runCommandAsync(Callback, String, int, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> runCommandAsync(final String commandName,
            final int commandValue, final DocumentAssignable options)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        runCommandAsync(future, commandName, commandValue, options);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call the
     * {@link #runCommandAsync(Callback, String, String, DocumentAssignable)}
     * method.
     * </p>
     *
     * @see #runCommandAsync(Callback, String, String, DocumentAssignable)
     */
    @Override
    public ListenableFuture<Document> runCommandAsync(final String commandName,
            final String commandValue, final DocumentAssignable options)
            throws MongoDbException {
        final FutureCallback<Document> future = new FutureCallback<Document>(
                getLockType());

        runCommandAsync(future, commandName, commandValue, options);

        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDurability(final Durability durability) {
        myDurability = durability;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to update the databases profile level.
     * </p>
     *
     * @see MongoDatabase#setProfilingStatus
     */
    @Override
    public boolean setProfilingStatus(final ProfilingStatus profileLevel)
            throws MongoDbException {
        final Document result = runCommand(
                "profile",
                profileLevel.getLevel().getValue(),
                BuilderFactory.start().add("slowms",
                        profileLevel.getSlowMillisThreshold()));

        final NumericElement level = result.get(NumericElement.class, "was");
        final NumericElement millis = result
                .get(NumericElement.class, "slowms");

        if ((level != null) && (millis != null)) {
            final ProfilingStatus.Level l = ProfilingStatus.Level
                    .fromValue(level.getIntValue());
            if (l != null) {
                switch (l) {
                case NONE:
                    return !ProfilingStatus.Level.NONE.equals(profileLevel
                            .getLevel());
                case ALL:
                    return !ProfilingStatus.Level.ALL.equals(profileLevel
                            .getLevel());
                case SLOW_ONLY:
                    final ProfilingStatus before = ProfilingStatus.slow(millis
                            .getIntValue());
                    return !before.equals(profileLevel);
                }
            }
        }

        // From undefined to defined is a change?
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadPreference(final ReadPreference readPreference) {
        myReadPreference = readPreference;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send a {@code dbStats} command to the MongoDB server.
     * </p>
     *
     * @see MongoDatabase#stats
     */
    @Override
    public Document stats() throws MongoDbException {
        return runCommand("dbStats");
    }

    /**
     * Adds the options to the document builder.
     *
     * @param command
     *            The command to make sure is removed from the options.
     * @param options
     *            The options to be added. May be <code>null</code>.
     * @param builder
     *            The builder to add the options to.
     */
    protected void addOptions(final String command,
            final DocumentAssignable options, final DocumentBuilder builder) {
        if (options != null) {
            for (final Element element : options.asDocument()) {
                if (!command.equals(element.getName())) {
                    builder.add(element);
                }
            }
        }
    }

    /**
     * Returns the type of lock to use.
     *
     * @return The type of lock to use.
     */
    protected LockType getLockType() {
        return myClient.getConfig().getLockType();
    }

    /**
     * Returns a {@link MongoDatabase} interface to the 'admin' database.
     *
     * @return A reference to a {@link MongoDatabase} for interacting with the
     *         'admin' database.
     */
    private MongoDatabase getAdminDatabase() {
        if (myAdminDatabase == null) {
            if (myName.equals("admin")) {
                myAdminDatabase = this;
            }
            else {
                myAdminDatabase = myMongoClient.getDatabase("admin");
            }
        }

        return myAdminDatabase;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #listCollectionsAsync(ListCollections)}.
     * </p>
     */
    @Override
    public MongoIterator<Document> listCollections(
            ListCollections listCollections) throws MongoDbException {
        return FutureUtils.unwrap(listCollectionsAsync(listCollections));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #listCollections(ListCollections)}.
     * </p>
     */
    @Override
    public MongoIterator<Document> listCollections(
            ListCollections.Builder listCollections) throws MongoDbException {
        return listCollections(listCollections.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #listCollectionsAsync(Callback, ListCollections.Builder)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> listCollectionsAsync(
            ListCollections listCollections) throws MongoDbException {
        FutureCallback<MongoIterator<Document>> future = new FutureCallback<MongoIterator<Document>>(
                getLockType());

        listCollectionsAsync(future, listCollections);

        return future;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #listCollectionsAsync(ListCollections)}.
     * </p>
     */
    @Override
    public ListenableFuture<MongoIterator<Document>> listCollectionsAsync(
            ListCollections.Builder listCollections) throws MongoDbException {
        return listCollectionsAsync(listCollections.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create the {@link ListCollectionsCommand} and send it to
     * the client.
     * </p>
     */
    @Override
    public void listCollectionsAsync(Callback<MongoIterator<Document>> results,
            ListCollections listCollections) throws MongoDbException {

        ReadPreference readPreference = listCollections.getReadPreference();
        if (readPreference == null) {
            readPreference = getReadPreference();
        }

        final ListCollectionsCommand command = new ListCollectionsCommand(
                getName(), listCollections, readPreference, myClient
                        .getClusterType().isSharded());

        final CursorCallback callback = new CursorCallback(myClient, command,
                true, results);

        myClient.send(command, callback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #listCollectionsAsync(Callback, ListCollections)}.
     * </p>
     */
    @Override
    public void listCollectionsAsync(Callback<MongoIterator<Document>> results,
            ListCollections.Builder listCollections) throws MongoDbException {
        listCollectionsAsync(results, listCollections.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #listCollectionsAsync(LambdaCallback, ListCollections)}.
     * </p>
     */
    @Override
    public void listCollectionsAsync(
            LambdaCallback<MongoIterator<Document>> results,
            ListCollections.Builder listCollections) throws MongoDbException {
        listCollectionsAsync(results, listCollections.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call
     * {@link #listCollectionsAsync(Callback, ListCollections)}.
     * </p>
     */
    @Override
    public void listCollectionsAsync(
            LambdaCallback<MongoIterator<Document>> results,
            ListCollections listCollections) throws MongoDbException {
        listCollectionsAsync(
                new LambdaCallbackAdapter<MongoIterator<Document>>(results),
                listCollections);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #stream(LambdaCallback, ListCollections)}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(LambdaCallback<Document> results,
            ListCollections.Builder listCollections) throws MongoDbException {
        return stream(results, listCollections.build());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #stream(StreamCallback, ListCollections)}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(LambdaCallback<Document> results,
            ListCollections listCollections) throws MongoDbException {
        return stream(new LambdaCallbackAdapter<Document>(results),
                listCollections);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to initiate the streaming of the collection messages and
     * return the {@link MongoCursorControl} for the stream.
     * </p>
     */
    @Override
    public MongoCursorControl stream(StreamCallback<Document> results,
            ListCollections listCollections) throws MongoDbException {
        ReadPreference readPreference = listCollections.getReadPreference();
        if (readPreference == null) {
            readPreference = getReadPreference();
        }

        final ListCollectionsCommand commandMsg = new ListCollectionsCommand(
                getName(), listCollections, readPreference, myClient
                        .getClusterType().isSharded());

        final CursorStreamingCallback callback = new CursorStreamingCallback(
                myClient, commandMsg, true, results);

        myClient.send(commandMsg, callback);

        return callback;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #stream(StreamCallback, ListCollections)}.
     * </p>
     */
    @Override
    public MongoCursorControl stream(StreamCallback<Document> results,
            ListCollections.Builder listCollections) throws MongoDbException {
        return stream(results, listCollections.build());
    }
}
