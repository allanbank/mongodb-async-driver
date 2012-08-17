/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.gridfs;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;
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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GridFs {

    /** The suffix for the chunks collection. */
    public static final String CHUNKS_SUFFIX = ".chunks";

    /** The default chunk size. */
    public static final int DEFAULT_CHUNK_SIZE = 256 * 1024;

    /** The suffix for the files collection. */
    public static final String DEFAULT_ROOT = "fs";

    /** The suffix for the files collection. */
    public static final String FILES_SUFFIX = ".files";

    /** The GridFS chunks collection. */
    private final MongoCollection myChunksCollection;

    /** The size for a chunk written. */
    private int myChunkSize = DEFAULT_CHUNK_SIZE;

    /** The GridFS files collection. */
    private final MongoCollection myFilesCollection;

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

        final MongoDatabase database = MongoFactory.create(uri).getDatabase(
                uri.getDatabase());
        myFilesCollection = database.getCollection(rootName + FILES_SUFFIX);
        myChunksCollection = database.getCollection(rootName + CHUNKS_SUFFIX);
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
     * <tt>sink</tt>
     * 
     * @param name
     *            The name of the file.
     * @param sink
     *            The stream to Write the data to.
     * @throws IOException
     *             On a failure reading the data from MongoDB or writing to the
     *             <tt>sink</tt>.
     */
    public void read(final String name, final OutputStream sink)
            throws IOException {
        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection.findOne(where("filename")
                .equals(name));
        if (fileDoc == null) {
            throw new FileNotFoundException(name);
        }

        final Element id = fileDoc.get("_id");
        final Element queryElement = id.withName("files_id");
        final DocumentBuilder queryDoc = BuilderFactory.start();
        queryDoc.add(queryElement);

        final Find.Builder findBuilder = new Find.Builder(queryDoc.build());
        findBuilder.setSort(BuilderFactory.start().addInteger("n", 1).build());

        // Small batch size since the docs are big and we can do parallel I/O.
        findBuilder.setBatchSize(2);

        final ClosableIterator<Document> iter = myChunksCollection
                .find(findBuilder.build());
        for (final Document chunk : iter) {
            for (final BinaryElement bytes : chunk.queryPath(
                    BinaryElement.class, "data")) {
                sink.write(bytes.getValue());
            }
        }
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
     * @param name
     *            The name of the file to be deleted.
     * @return True if a file was deleted, false otherwise.
     * @throws IOException
     *             On a failure to delete the file.
     */
    public boolean unlink(final String name) throws IOException {
        // Find the document with the specified name.
        final Document fileDoc = myFilesCollection.findOne(where("filename")
                .equals(name));
        if (fileDoc == null) {
            return false;
        }

        final Element id = fileDoc.get("_id");

        final DocumentBuilder queryDoc = BuilderFactory.start();
        queryDoc.add(id.withName("files_id"));
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
     * Attempts to write a file into the GridFS collections using the specified
     * name for the file and deriving the chunks from the data read from the
     * <tt>source</tt>.
     * 
     * @param name
     *            The name of the file being written.
     * @param source
     *            The source of the bits in the file.
     * @throws IOException
     *             On a failure writing the documents or reading the file
     *             contents. In the case of a failure an attempt is made to
     *             remove the documents written to the collections.
     */
    public void write(final String name, final InputStream source)
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
                doc.reset();

                final ObjectId chunkId = new ObjectId();
                doc.addObjectId("_id", chunkId);
                doc.addObjectId("files_id", id);
                doc.addInteger("n", n);

                final byte[] data = Arrays.copyOf(buffer, read);
                md5Digest.update(data);
                doc.addBinary("data", data);

                results.add(myChunksCollection.insertAsync(doc.build()));

                length += data.length;
                read = readFully(source, buffer);
                n += 0;
            }

            doc.reset();
            doc.addObjectId("_id", id);
            doc.addString("filename", name);
            doc.addTimestamp("uploadDate", System.currentTimeMillis());
            doc.addInteger("chunkSize", buffer.length);
            doc.addLong("length", length);
            doc.addString("md5", IOUtils.toHex(md5Digest.digest()));

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
            throw new IOException(e);
        }
        catch (final ExecutionException e) {
            failed = true;
            throw new IOException(e.getCause());
        }
        finally {
            if (failed) {
                myFilesCollection.delete(where("_id").equals(id));
                myChunksCollection.delete(where("files_id").equals(id));
            }
        }
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
