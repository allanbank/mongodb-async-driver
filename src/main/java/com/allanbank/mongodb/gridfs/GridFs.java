/*
 * #%L
 * GridFs.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.gridfs;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.util.IOUtils;

/**
 * GridFs provides an interface for working with a GridFS collection.
 * <p>
 * This implementation uses a {@link ObjectId} as the id when writing and stores
 * the name of the file in the files collection document's "filename" field. To
 * {@link #unlink(String)} or {@link #read(String, OutputStream)} a file from
 * the collection the _id field may contain any value but the filename field
 * must be present.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GridFs {

    /**
     * The field in the {@link #CHUNKS_SUFFIX chunks} collection containing the
     * chunk's number.
     */
    public static final String CHUNK_NUMBER_FIELD = "n";

    /** The amount of overhead in a chunk document in bytes: {@value} */
    public static final int CHUNK_OVERHEAD = 58;

    /**
     * The field in the {@link #FILES_SUFFIX files} collection containing the
     * file's chunk size.
     */
    public static final String CHUNK_SIZE_FIELD = "chunkSize";

    /** The suffix for the chunks collection. */
    public static final String CHUNKS_SUFFIX = ".chunks";

    /**
     * The field in the {@link #CHUNKS_SUFFIX chunks} collection containing the
     * chunk's data.
     */
    public static final String DATA_FIELD = "data";

    /**
     * The default chunk size. This is slightly less than 256K to allow for the
     * {@link #CHUNK_OVERHEAD} when using the power of two allocator.
     */
    public static final int DEFAULT_CHUNK_SIZE;

    /** The suffix for the files collection. */
    public static final String DEFAULT_ROOT = "fs";

    /**
     * The field in the {@link #FILES_SUFFIX files} collection containing the
     * file's name.
     */
    public static final String FILENAME_FIELD = "filename";

    /**
     * The field in the {@link #CHUNKS_SUFFIX chunks} collection containing the
     * chunk's related file id.
     */
    public static final String FILES_ID_FIELD = "files_id";

    /** The suffix for the files collection. */
    public static final String FILES_SUFFIX = ".files";

    /** The {@code _id} field name. */
    public static final String ID_FIELD = "_id";

    /**
     * The field in the {@link #FILES_SUFFIX files} collection containing the
     * file's length.
     */
    public static final String LENGTH_FIELD = "length";

    /**
     * The field in the {@link #FILES_SUFFIX files} collection containing the
     * file's MD5.
     */
    public static final String MD5_FIELD = "md5";

    /**
     * The field in the {@link #FILES_SUFFIX files} collection containing the
     * file's upload date.
     */
    public static final String UPLOAD_DATE_FIELD = "uploadDate";

    static {
        DEFAULT_CHUNK_SIZE = (256 * 1024) - CHUNK_OVERHEAD;
    }

    /** The GridFS chunks collection. */
    private final MongoCollection myChunksCollection;

    /** The size for a chunk written. */
    private int myChunkSize = DEFAULT_CHUNK_SIZE;

    /** The GridFS database. */
    private final MongoDatabase myDatabase;

    /** The GridFS files collection. */
    private final MongoCollection myFilesCollection;

    /** The root name for the GridFS collections. */
    private final String myRootName;

    /**
     * Creates a new GridFs.
     * <p>
     * The GridFS objects will be stored in the 'fs' collection.
     * </p>
     * 
     * @param database
     *            The database containing the GridFS collections.
     */
    public GridFs(final MongoDatabase database) {
        this(database, DEFAULT_ROOT);
    }

    /**
     * Creates a new GridFs.
     * 
     * 
     * @param database
     *            The database containing the GridFS collections.
     * @param rootName
     *            The rootName for the collections. The {@link #FILES_SUFFIX}
     *            and {@link #CHUNKS_SUFFIX} will be appended to create the two
     *            collection names.
     */
    public GridFs(final MongoDatabase database, final String rootName) {
        myRootName = rootName;
        myDatabase = database;
        myFilesCollection = database.getCollection(rootName + FILES_SUFFIX);
        myChunksCollection = database.getCollection(rootName + CHUNKS_SUFFIX);
    }

    /**
     * Creates a new GridFs.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @throws IllegalArgumentException
     *             If the <tt>mongoDbUri</tt> is not a properly formated MongoDB
     *             style URL.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public GridFs(final String mongoDbUri) {
        this(mongoDbUri, DEFAULT_ROOT);
    }

    /**
     * Creates a new GridFs.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @param rootName
     *            The rootName for the collections. The {@link #FILES_SUFFIX}
     *            and {@link #CHUNKS_SUFFIX} will be appended to create the two
     *            collection names.
     * @throws IllegalArgumentException
     *             If the <tt>mongoDbUri</tt> is not a properly formated MongoDB
     *             style URL.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public GridFs(final String mongoDbUri, final String rootName) {
        final MongoDbUri uri = new MongoDbUri(mongoDbUri);

        final MongoDatabase database = MongoFactory.createClient(uri)
                .getDatabase(uri.getDatabase());

        myRootName = rootName;
        myDatabase = database;
        myFilesCollection = database.getCollection(rootName + FILES_SUFFIX);
        myChunksCollection = database.getCollection(rootName + CHUNKS_SUFFIX);
    }

    /**
     * Creates the following indexes:
     * <ul>
     * <li>
     * Files Collection:
     * <ul>
     * <li><code>{ 'filename' : 1, 'uploadDate' : 1 }</code></li>
     * </ul>
     * </li>
     * <li>
     * Chunks Collection:
     * <ul>
     * <li><code>{ 'files_id' : 1, 'n' : 1 }</code></li>
     * </ul>
     * </li>
     * </ul>
     * If in a non-sharded environment the indexes will be unique.
     */
    public void createIndexes() {
        try {
            myFilesCollection.createIndex(true, Index.asc(FILENAME_FIELD),
                    Index.asc(UPLOAD_DATE_FIELD));
        }
        catch (final MongoDbException error) {
            // Can't be unique in a sharded environment.
            myFilesCollection.createIndex(false, Index.asc(FILENAME_FIELD),
                    Index.asc(UPLOAD_DATE_FIELD));
        }

        try {
            myChunksCollection.createIndex(true, Index.asc(FILES_ID_FIELD),
                    Index.asc(CHUNK_NUMBER_FIELD));
        }
        catch (final MongoDbException error) {
            // Can't be unique in a sharded environment.
            myChunksCollection.createIndex(false, Index.asc(FILES_ID_FIELD),
                    Index.asc(CHUNK_NUMBER_FIELD));
        }
    }

    /**
     * Validates and optionally tries to repair the GridFS collections.
     * <ul>
     * <li>
     * Ensure the following indexes exist:
     * <ul>
     * <li>
     * Files Collection:
     * <ul>
     * <li><code>{ 'filename' : 1, 'uploadDate' : 1 }</code></li>
     * </ul>
     * </li>
     * <li>
     * Chunks Collection:
     * <ul>
     * <li><code>{ 'files_id' : 1, 'n' : 1 }</code></li>
     * </ul>
     * </li>
     * </ul>
     * </li>
     * <li>
     * Ensure there are no duplicate {@code n} values for the chunks of a file.
     * If {@code repair} is true then the {@code n} values will be updated to be
     * sequential based on the ordering <tt>{ 'n' : 1, '_id' 1 }</tt>.</li>
     * <li>
     * Validates the MD5 sum for each file via the <a
     * href="http://docs.mongodb.org/manual/reference/command/filemd5"
     * >filemd5</a> command.</li>
     * </ul>
     * <p>
     * <b>Warning:</b> This function iterates over every file in the GridFS
     * collection and can take a considerable amount of time and resources on
     * the client and the server.
     * </p>
     * <p>
     * <b>Note:</b> Due to a limitation in the MongoDB server this method will
     * return false positives when used with a sharded cluster when the shard
     * key for the chunks collection is not one of <code>{files_id:1}</code> or
     * <code>{files_id:1, n:1}</code>. See <a
     * href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>.
     * </p>
     * 
     * @param repair
     *            If set to <code>true</code> then the fsck will attempt to
     *            repair common errors.
     * @return A map of the file ids to the errors found for the file and the
     *         repair status. If no errors are found an empty map is returned.
     * @throws IOException
     *             On a failure to execute the fsck.
     * 
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>
     */
    public Map<Object, List<String>> fsck(final boolean repair)
            throws IOException {

        final Map<Object, List<String>> faults = new HashMap<Object, List<String>>();

        createIndexes();

        // Use the filemd5 command to locate files to inspect more closely.
        final MongoIterator<Document> iter = myFilesCollection.find(Find.ALL);
        try {
            for (final Document fileDoc : iter) {
                final Element id = fileDoc.get(ID_FIELD);

                final DocumentBuilder commandDoc = BuilderFactory.start();
                commandDoc.add(id.withName("filemd5"));
                commandDoc.add("root", myRootName);

                final Document commandResult = myDatabase.runCommand(commandDoc
                        .build());
                if (!doVerifyFileMd5(faults, fileDoc, commandResult) && repair) {
                    doTryAndRepair(fileDoc, faults);
                }
            }
        }
        finally {
            iter.close();
        }
        return faults;
    }

    /**
     * Returns the size for a chunk written.
     * 
     * @return The size for a chunk written.
     */
    public int getChunkSize() {
        return myChunkSize;
    }

    /**
     * Reads a file from the GridFS collections and writes the contents to the
     * {@code sink}
     * 
     * @param id
     *            The id of the file.
     * @param sink
     *            The stream to write the data to. This stream will not be
     *            closed by this method.
     * @throws IOException
     *             On a failure reading the data from MongoDB or writing to the
     *             {@code sink}.
     */
    public void read(final ObjectId id, final OutputStream sink)
            throws IOException {
        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
                .equals(id));
        if (fileDoc == null) {
            throw new FileNotFoundException(id.toString());
        }

        doRead(fileDoc, sink);
    }

    /**
     * Reads a file from the GridFS collections and writes the contents to the
     * {@code sink}
     * 
     * @param name
     *            The name of the file.
     * @param sink
     *            The stream to write the data to. This stream will not be
     *            closed by this method.
     * @throws IOException
     *             On a failure reading the data from MongoDB or writing to the
     *             {@code sink}.
     */
    public void read(final String name, final OutputStream sink)
            throws IOException {

        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection
                .findOne(where(FILENAME_FIELD).equals(name));
        if (fileDoc == null) {
            throw new FileNotFoundException(name);
        }

        doRead(fileDoc, sink);
    }

    /**
     * Sets the value of size for a chunk written.
     * 
     * @param chunkSize
     *            The new value for the size for a chunk written.
     */
    public void setChunkSize(final int chunkSize) {
        myChunkSize = chunkSize;
    }

    /**
     * Unlinks (deletes) the file from the GridFS collections.
     * 
     * @param id
     *            The id of the file to be deleted.
     * @return True if a file was deleted, false otherwise.
     * @throws IOException
     *             On a failure to delete the file.
     */
    public boolean unlink(final ObjectId id) throws IOException {

        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
                .equals(id));
        if (fileDoc == null) {
            return false;
        }

        return doUnlink(fileDoc);
    }

    /**
     * Unlinks (deletes) the file from the GridFS collections.
     * 
     * @param name
     *            The name of the file to be deleted.
     * @return True if a file was deleted, false otherwise.
     * @throws IOException
     *             On a failure to validate the file.
     */
    public boolean unlink(final String name) throws IOException {

        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection
                .findOne(where(FILENAME_FIELD).equals(name));
        if (fileDoc == null) {
            return false;
        }

        return doUnlink(fileDoc);
    }

    /**
     * Validates the file from the GridFS collections using the {@code filemd5}
     * command.
     * <p>
     * <b>Note:</b> Due to a limitation in the MongoDB server this method will
     * always return <code>false</code> when used with a sharded cluster when
     * the shard key for the chunks collection is not one of
     * <code>{files_id:1}</code> or <code>{files_id:1, n:1}</code>. See <a
     * href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>.
     * </p>
     * 
     * @param id
     *            The id of the file to be validate.
     * @return True if a file was validated (md5 hash matches), false otherwise.
     * @throws IOException
     *             On a failure to validate the file.
     * 
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>
     */
    public boolean validate(final ObjectId id) throws IOException {

        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
                .equals(id));
        if (fileDoc == null) {
            throw new FileNotFoundException(id.toString());
        }

        return doValidate(fileDoc);
    }

    /**
     * Validates the file from the GridFS collections using the {@code filemd5}
     * command.
     * <p>
     * <b>Note:</b> Due to a limitation in the MongoDB server this method will
     * always return <code>false</code> when used with a sharded cluster when
     * the shard key for the chunks collection is not one of
     * <code>{files_id:1}</code> or <code>{files_id:1, n:1}</code>. See <a
     * href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>.
     * </p>
     * 
     * @param name
     *            The name of the file to be validate.
     * @return True if a file was validated (md5 hash matches), false otherwise.
     * @throws IOException
     *             On a failure to validate the file.
     * 
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>
     */
    public boolean validate(final String name) throws IOException {

        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection
                .findOne(where(FILENAME_FIELD).equals(name));
        if (fileDoc == null) {
            throw new FileNotFoundException(name);
        }

        return doValidate(fileDoc);
    }

    /**
     * Attempts to write a file into the GridFS collections using the specified
     * name for the file and deriving the chunks from the data read from the
     * <tt>source</tt>.
     * 
     * @param name
     *            The name of the file being written.
     * @param source
     *            The source of the bits in the file. This stream will not be
     *            closed.
     * @return The {@link ObjectId} associted with the file.
     * @throws IOException
     *             On a failure writing the documents or reading the file
     *             contents. In the case of a failure an attempt is made to
     *             remove the documents written to the collections.
     */
    public ObjectId write(final String name, final InputStream source)
            throws IOException {
        final ObjectId id = new ObjectId();
        boolean failed = false;
        try {
            final byte[] buffer = new byte[myChunkSize];
            final MessageDigest md5Digest = MessageDigest.getInstance("MD5");

            final List<Future<Integer>> results = new ArrayList<Future<Integer>>();
            final DocumentBuilder doc = BuilderFactory.start();
            int n = 0;
            long length = 0;
            int read = readFully(source, buffer);
            while (read > 0) {

                final ObjectId chunkId = new ObjectId();

                doc.reset();
                doc.addObjectId(ID_FIELD, chunkId);
                doc.addObjectId(FILES_ID_FIELD, id);
                doc.addInteger(CHUNK_NUMBER_FIELD, n);

                final byte[] data = (read == buffer.length) ? buffer : Arrays
                        .copyOf(buffer, read);
                md5Digest.update(data);
                doc.addBinary(DATA_FIELD, data);

                results.add(myChunksCollection.insertAsync(doc.build()));

                length += data.length;
                read = readFully(source, buffer);
                n += 1;
            }

            doc.reset();
            doc.addObjectId(ID_FIELD, id);
            doc.addString(FILENAME_FIELD, name);
            doc.addTimestamp(UPLOAD_DATE_FIELD, System.currentTimeMillis());
            doc.addInteger(CHUNK_SIZE_FIELD, buffer.length);
            doc.addLong(LENGTH_FIELD, length);
            doc.addString(MD5_FIELD, IOUtils.toHex(md5Digest.digest()));

            results.add(myFilesCollection.insertAsync(doc.build()));

            // Make sure everything made it to the server.
            for (final Future<Integer> f : results) {
                f.get();
            }
        }
        catch (final NoSuchAlgorithmException e) {
            failed = true;
            throw new IOException(e);
        }
        catch (final InterruptedException e) {
            failed = true;
            final InterruptedIOException error = new InterruptedIOException(
                    e.getMessage());
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            failed = true;
            throw new IOException(e.getCause());
        }
        finally {
            if (failed) {
                myFilesCollection.delete(where(ID_FIELD).equals(id));
                myChunksCollection.delete(where(FILES_ID_FIELD).equals(id));
            }
        }

        return id;
    }

    /**
     * Adds a fault message to the faults map.
     * 
     * @param faults
     *            The map of file ids to the error messages.
     * @param idObj
     *            The id for the file.
     * @param message
     *            The message to add.
     */
    protected void doAddFault(final Map<Object, List<String>> faults,
            final Element idObj, final String message) {
        List<String> docFaults = faults.get(idObj.getValueAsObject());
        if (docFaults == null) {
            docFaults = new ArrayList<String>();
            faults.put(idObj.getValueAsObject(), docFaults);
        }
        docFaults.add(message);
    }

    /**
     * Reads a file from the GridFS collections and writes the contents to the
     * {@code sink}
     * 
     * @param fileDoc
     *            The document for the file.
     * @param sink
     *            The stream to write the data to. This stream will not be
     *            closed by this method.
     * @throws IOException
     *             On a failure reading the data from MongoDB or writing to the
     *             {@code sink}.
     */
    protected void doRead(final Document fileDoc, final OutputStream sink)
            throws IOException {

        final Element id = fileDoc.get(ID_FIELD);

        long length = -1;
        final NumericElement lengthElement = fileDoc.get(NumericElement.class,
                LENGTH_FIELD);
        if (lengthElement != null) {
            length = lengthElement.getLongValue();
        }

        long chunkSize = -1;
        final NumericElement chunkSizeElement = fileDoc.get(
                NumericElement.class, CHUNK_SIZE_FIELD);
        if (chunkSizeElement != null) {
            chunkSize = chunkSizeElement.getLongValue();
        }

        long numberChunks = -1;
        if ((0 <= length) && (0 < chunkSize)) {
            numberChunks = (long) Math.ceil((double) length
                    / (double) chunkSize);
        }

        final Element queryElement = id.withName(FILES_ID_FIELD);
        final DocumentBuilder queryDoc = BuilderFactory.start();
        queryDoc.add(queryElement);

        final Find.Builder findBuilder = new Find.Builder(queryDoc.build());
        findBuilder.setSort(asc(CHUNK_NUMBER_FIELD));

        // Small batch size since the docs are big and we can do parallel I/O.
        findBuilder.setBatchSize(2);

        long expectedChunk = 0;
        long totalSize = 0;
        final MongoIterator<Document> iter = myChunksCollection
                .find(findBuilder.build());
        try {
            for (final Document chunk : iter) {

                final NumericElement n = chunk.get(NumericElement.class,
                        CHUNK_NUMBER_FIELD);
                final BinaryElement bytes = chunk.get(BinaryElement.class,
                        DATA_FIELD);

                if (n == null) {
                    throw new IOException("Missing chunk number '"
                            + (expectedChunk + 1) + "' of '" + numberChunks
                            + "'.");
                }
                else if (n.getLongValue() != expectedChunk) {
                    throw new IOException("Skipped chunk '"
                            + (expectedChunk + 1) + "', retreived '"
                            + n.getLongValue() + "' of '" + numberChunks + "'.");
                }
                else if (bytes == null) {
                    throw new IOException("Missing bytes in chunk '"
                            + (expectedChunk + 1) + "' of '" + numberChunks
                            + "'.");
                }
                else {

                    final byte[] buffer = bytes.getValue();

                    sink.write(buffer);
                    expectedChunk += 1;
                    totalSize += buffer.length;
                }
            }
        }
        finally {
            iter.close();
            sink.flush();
        }

        if ((0 <= numberChunks) && (expectedChunk < numberChunks)) {
            throw new IOException("Missing chunks after '" + expectedChunk
                    + "' of '" + numberChunks + "'.");
        }
        if ((0 <= length) && (totalSize != length)) {
            throw new IOException("File size mismatch. Expected '" + length
                    + "' but only read '" + totalSize + "' bytes.");
        }
    }

    /**
     * Tries to repair the file.
     * <p>
     * Currently the only strategy is to reorder the chunk's into _id order. The
     * operation verifies that the reorder fixes the file prior to modifying
     * anything. it also verifies that the reordering worked after reordering
     * the chunks.
     * 
     * @param fileDoc
     *            The document representing the file.
     * @param faults
     *            The map to update with the status of the repair.
     */
    protected void doTryAndRepair(final Document fileDoc,
            final Map<Object, List<String>> faults) {
        // First see if the MD5 for the file's chunks in _id order returns the
        // right results.
        final List<Element> chunkIds = new ArrayList<Element>();

        final Element id = fileDoc.get(ID_FIELD);
        final Element md5 = fileDoc.get(MD5_FIELD);
        final Element queryElement = id.withName(FILES_ID_FIELD);
        final DocumentBuilder queryDoc = BuilderFactory.start().add(
                queryElement);

        final Find.Builder findBuilder = new Find.Builder(queryDoc.build());
        findBuilder.setSort(asc(ID_FIELD));

        // Small batch size since the docs are big and we can do parallel I/O.
        findBuilder.setBatchSize(2);

        MongoIterator<Document> iter = null;
        try {
            final MessageDigest md5Digest = MessageDigest.getInstance("MD5");
            iter = myChunksCollection.find(findBuilder);
            for (final Document chunkDoc : iter) {

                chunkIds.add(chunkDoc.get(ID_FIELD));

                final BinaryElement chunk = chunkDoc.get(BinaryElement.class,
                        DATA_FIELD);
                if (chunk != null) {
                    md5Digest.update(chunk.getValue());
                }
            }

            final String digest = IOUtils.toHex(md5Digest.digest());
            final StringElement computed = new StringElement(MD5_FIELD, digest);
            if (computed.equals(md5)) {
                // Update the 'n' fields for each chunk to be in the right
                // order.
                int n = 0;
                for (final Element idElement : chunkIds) {
                    final DocumentBuilder query = BuilderFactory.start();
                    query.add(idElement);
                    query.add(queryElement); // Direct to the right shard.

                    final DocumentBuilder update = BuilderFactory.start();
                    update.push("$set").add(CHUNK_NUMBER_FIELD, n);

                    // Use a multi-update to ensure the write happens when a
                    // files chunks are across shards.
                    myChunksCollection.update(query.build(), update.build(),
                            true /* =multi */, false, Durability.ACK);

                    n += 1;
                }

                if (doValidate(fileDoc)) {
                    doAddFault(faults, id, "File repaired.");

                }
                else {
                    doAddFault(faults, id,
                            "Repair failed: Chunks reordered but sill not validating.");
                }
            }
            else {
                doAddFault(faults, id,
                        "Repair failed: Could not determine correct chunk order.");
            }
        }
        catch (final NoSuchAlgorithmException e) {
            doAddFault(faults, id,
                    "Repair failed: Could not compute the MD5 for the file: "
                            + e.getMessage());
        }
        catch (final RuntimeException e) {
            doAddFault(faults, id, "Potential Repair Failure: Runtime error: "
                    + e.getMessage());
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Unlinks (deletes) the file from the GridFS collections.
     * 
     * @param fileDoc
     *            The document for the file to delete.
     * @return True if a file was deleted, false otherwise.
     * @throws IOException
     *             On a failure to delete the file.
     */
    protected boolean doUnlink(final Document fileDoc) throws IOException {
        final Element id = fileDoc.get(ID_FIELD);

        final DocumentBuilder queryDoc = BuilderFactory.start();
        queryDoc.add(id.withName(FILES_ID_FIELD));
        final Future<Long> cFuture = myChunksCollection.deleteAsync(queryDoc);

        queryDoc.reset();
        queryDoc.add(id);
        final Future<Long> fFuture = myFilesCollection.deleteAsync(queryDoc);

        try {
            return (cFuture.get().longValue() >= 0)
                    && (fFuture.get().longValue() > 0);
        }
        catch (final InterruptedException e) {
            return false;
        }
        catch (final ExecutionException e) {
            return false;
        }
    }

    /**
     * Validates the file from the GridFS collections using the {@code filemd5}
     * command.
     * <p>
     * <b>Note:</b> Due to a limitation in the MongoDB server this method will
     * always return <code>false</code> when used with a sharded cluster when
     * the shard key for the chunks collection is not one of
     * <code>{files_id:1}</code> or <code>{files_id:1, n:1}</code>. See <a
     * href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>.
     * </p>
     * 
     * @param fileDoc
     *            The document for the file to delete.
     * @return True if a file was deleted, false otherwise.
     * 
     * @see <a
     *      href="https://jira.mongodb.org/browse/SERVER-9888">SERVER-9888</a>
     */
    protected boolean doValidate(final Document fileDoc) {
        final Element id = fileDoc.get(ID_FIELD);
        final Element md5 = fileDoc.get(MD5_FIELD);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add(id.withName("filemd5"));
        commandDoc.add("root", myRootName);
        final Document result = myDatabase.runCommand(commandDoc.build());

        return (md5 != null) && md5.equals(result.findFirst(MD5_FIELD));
    }

    /**
     * Verifies the MD5 result for the filemd5 command.
     * 
     * @param faults
     *            The faults for to update if the verify fails.
     * @param fileDoc
     *            The document representing the file.
     * @param cmdResult
     *            The document returned from the 'filemd5' command.
     * @return True if the file was successful.
     */
    protected boolean doVerifyFileMd5(final Map<Object, List<String>> faults,
            final Document fileDoc, final Document cmdResult) {
        boolean ok = false;
        final Element idElement = fileDoc.get(ID_FIELD);

        final Element md5 = fileDoc.get(MD5_FIELD);
        final Element commandMd5 = cmdResult.findFirst(MD5_FIELD);

        ok = (md5 != null) && md5.equals(commandMd5);
        if (!ok) {
            doAddFault(faults, idElement,
                    "MD5 sums do not match. File document contains '" + md5
                            + "' and the filemd5 command produced '"
                            + commandMd5 + "'.");
        }

        return ok;
    }

    /**
     * Read the full contents of the stream until an EOF into the buffer.
     * 
     * @param source
     *            The source if bytes to read.
     * @param buffer
     *            The buffer to read into.
     * @return The number of bytes read. If less than <tt>buffer.length</tt>
     *         then the stream reach the end-of-file.
     * @throws IOException
     *             On a failure reading from the stream.
     */
    private int readFully(final InputStream source, final byte[] buffer)
            throws IOException {

        int offset = 0;

        while (true) {
            final int read = source
                    .read(buffer, offset, buffer.length - offset);
            if (read < 0) {
                return offset;
            }

            offset += read;

            if (offset == buffer.length) {
                return offset;
            }
        }
    }
}
