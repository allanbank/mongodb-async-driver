/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.gridfs;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.connection.FutureCallback;

/**
 * GridFsTest provides tests for the {@link GridFs} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GridFsTest {

    /**
     * Test method for {@link GridFs#getChunkSize}.
     */
    @Test
    public void testChunkSize() {
        final GridFs fs = new GridFs("mongodb://localhost:27017/foo");

        assertEquals(GridFs.DEFAULT_CHUNK_SIZE, fs.getChunkSize());
        fs.setChunkSize(1024);
        assertEquals(1024, fs.getChunkSize());
    }

    /**
     * Test method for {@link GridFs#GridFs(MongoDatabase)}.
     */
    @Test
    public void testGridFsMongoDatabase() {
        final MongoDatabase mockDb = EasyMock.createMock(MongoDatabase.class);
        final MongoCollection mockFiles = EasyMock
                .createMock(MongoCollection.class);
        final MongoCollection mockChunks = EasyMock
                .createMock(MongoCollection.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertEquals(GridFs.DEFAULT_CHUNK_SIZE, fs.getChunkSize());

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#GridFs(String)}.
     */
    @Test
    public void testGridFsString() {
        final GridFs fs = new GridFs("mongodb://localhost:27017/foo");
        assertEquals(GridFs.DEFAULT_CHUNK_SIZE, fs.getChunkSize());
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testRead() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockChunks.find(anyObject(Find.class))).andReturn(mockIterator);
        expect(mockIterator.iterator()).andReturn(mockIterator);
        expect(mockIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockIterator.next()).andReturn(chunkResult.build());
        expect(mockIterator.hasNext()).andReturn(Boolean.FALSE);

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        fs.read("foo", sink);
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, sink.toByteArray());

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testReadNotFound() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(null);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
        }
        catch (final FileNotFoundException fnfe) {
            // Excellent.
        }

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlink() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Long> futureChunks = new FutureCallback<Long>();
        final FutureCallback<Long> futureFiles = new FutureCallback<Long>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockChunks.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Long.valueOf(1234));

        expect(mockFiles.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Long.valueOf(1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertTrue(fs.unlink("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkFailesOnFiles() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Long> futureChunks = new FutureCallback<Long>();
        final FutureCallback<Long> futureFiles = new FutureCallback<Long>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockChunks.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Long.valueOf(1234));

        expect(mockFiles.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Long.valueOf(-1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.unlink("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkFailesOnFilesLookup() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(null);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.unlink("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkFailsOnChunks() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Long> futureChunks = new FutureCallback<Long>();
        final FutureCallback<Long> futureFiles = new FutureCallback<Long>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockChunks.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Long.valueOf(-1));

        expect(mockFiles.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Long.valueOf(1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.unlink("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkFailsOnChunksException() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Long> futureChunks = new FutureCallback<Long>();
        final FutureCallback<Long> futureFiles = new FutureCallback<Long>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockChunks.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.exception(new RuntimeException());

        expect(mockFiles.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Long.valueOf(1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.unlink("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#write}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testWrite() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Integer> futureChunks = new FutureCallback<Integer>();
        final FutureCallback<Integer> futureFiles = new FutureCallback<Integer>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockChunks.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Integer.valueOf(1));

        expect(mockFiles.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Integer.valueOf(1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayInputStream in = new ByteArrayInputStream(new byte[] {
                1, 2, 3, 4 });
        fs.write("foo", in);

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#write}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testWriteMoreThan1Chunk() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Integer> futureChunks = new FutureCallback<Integer>();
        final FutureCallback<Integer> futureFiles = new FutureCallback<Integer>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockChunks.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Integer.valueOf(1));

        expect(mockChunks.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Integer.valueOf(1));

        expect(mockFiles.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.callback(Integer.valueOf(1));

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayInputStream in = new ByteArrayInputStream(
                new byte[fs.getChunkSize() + 1]);
        fs.write("foo", in);

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#write}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testWriteWithError() throws IOException {
        final RuntimeException thrown = new RuntimeException();
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 });

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final FutureCallback<Integer> futureChunks = new FutureCallback<Integer>();
        final FutureCallback<Integer> futureFiles = new FutureCallback<Integer>();

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockChunks.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureChunks);
        futureChunks.callback(Integer.valueOf(1));

        expect(mockFiles.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);
        futureFiles.exception(thrown);

        expect(mockChunks.delete(anyObject(DocumentAssignable.class)))
                .andReturn(0L);
        expect(mockFiles.delete(anyObject(DocumentAssignable.class)))
                .andReturn(0L);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayInputStream in = new ByteArrayInputStream(
                new byte[fs.getChunkSize()]);
        try {
            fs.write("foo", in);
            fail("Should have thrown an exception.");
        }
        catch (final IOException error) {
            // Good.
            assertSame(thrown, error.getCause());
        }
        verify(mockDb, mockFiles, mockChunks);
    }
}
