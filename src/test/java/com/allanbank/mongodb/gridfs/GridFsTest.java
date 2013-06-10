/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.gridfs;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Index;
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
     * Test method for {@link GridFs#createIndexes()}.
     */
    @Test
    public void testCreateIndex() {
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

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        fs.createIndexes();

        verify(mockDb, mockFiles, mockChunks);

    }

    /**
     * Test method for {@link GridFs#createIndexes()}.
     */
    @Test
    public void testCreateIndexUniqueFailes() {
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

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall().andThrow(new MongoDbException("Unique Failes."));
        mockFiles.createIndex(false, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall().andThrow(new MongoDbException("Unique Failes."));
        mockChunks.createIndex(false, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        fs.createIndexes();

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#doAddFault}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testDoAddFault() throws IOException {
        final Map<Object, List<String>> faults = new HashMap<Object, List<String>>();

        final GridFs gridFs = new GridFs("mongodb://localhost:27017/db");

        gridFs.doAddFault(faults, new StringElement("_id", "foo"), "Help.");
        assertEquals(
                Collections.singletonMap("foo",
                        Collections.singletonList("Help.")), faults);

        gridFs.doAddFault(faults, new StringElement("_id", "foo"),
                "Still not good.");
        assertEquals(
                Collections.singletonMap("foo",
                        Arrays.asList("Help.", "Still not good.")), faults);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsck() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(false);
        assertThat(results, is(Collections.EMPTY_MAP));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5Fails() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "abcdef12");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(false);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Collections
                                .singletonList("MD5 sums do not match. File document contains "
                                        + "'md5 : 'abcdef12'' and the filemd5 command produced 'md5 : 'abcdef''."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5FailsAttemptRepair() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "abcdef12");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);
        final MongoIterator<Document> mockChunkIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockChunks.find(anyObject(Find.Builder.class))).andReturn(
                mockChunkIterator);
        expect(mockChunkIterator.iterator()).andReturn(mockChunkIterator);
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockChunkIterator.next()).andReturn(chunkResult.build());
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.FALSE);
        mockChunkIterator.close();
        expectLastCall();

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(true);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Arrays.asList(
                                "MD5 sums do not match. File document contains "
                                        + "'md5 : 'abcdef12'' and the filemd5 command produced 'md5 : 'abcdef''.",
                                "Repair failed: Could not determine correct chunk order."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5FailsAttemptRepairThrowsRuntimeException()
            throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "abcdef12");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);
        final MongoIterator<Document> mockChunkIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockChunks.find(anyObject(Find.Builder.class))).andThrow(
                new RuntimeException("Injected"));

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(true);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Arrays.asList(
                                "MD5 sums do not match. File document contains "
                                        + "'md5 : 'abcdef12'' and the filemd5 command produced 'md5 : 'abcdef''.",
                                "Potential Repair Failure: Runtime error: Injected"))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5FailsAttemptRepairWithChunkMissingData()
            throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "abcdef12");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.add(GridFs.CHUNK_NUMBER_FIELD, 0);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);
        final MongoIterator<Document> mockChunkIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockChunks.find(anyObject(Find.Builder.class))).andReturn(
                mockChunkIterator);
        expect(mockChunkIterator.iterator()).andReturn(mockChunkIterator);
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockChunkIterator.next()).andReturn(chunkResult.build());
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.FALSE);
        mockChunkIterator.close();
        expectLastCall();

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(true);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Arrays.asList(
                                "MD5 sums do not match. File document contains "
                                        + "'md5 : 'abcdef12'' and the filemd5 command produced 'md5 : 'abcdef''.",
                                "Repair failed: Could not determine correct chunk order."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5FailsForMissingFileMD5() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(false);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Collections
                                .singletonList("MD5 sums do not match. File document contains "
                                        + "'null' and the filemd5 command produced 'md5 : 'abcdef''."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5Repair() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "08d6c05a21512a79a1dfeb9d2a8f262f");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "123456");

        final DocumentBuilder fixedCmdResult = BuilderFactory.start();
        fixedCmdResult.add("ok", 1).add(GridFs.MD5_FIELD,
                "08d6c05a21512a79a1dfeb9d2a8f262f");

        final ObjectId chunkId = new ObjectId();
        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.add("_id", chunkId)
                .addBinary("data", new byte[] { 1, 2, 3, 4 })
                .add(GridFs.CHUNK_NUMBER_FIELD, 0);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);
        final MongoIterator<Document> mockChunkIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockChunks.find(anyObject(Find.Builder.class))).andReturn(
                mockChunkIterator);
        expect(mockChunkIterator.iterator()).andReturn(mockChunkIterator);
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockChunkIterator.next()).andReturn(chunkResult.build());
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.FALSE);
        mockChunkIterator.close();
        expectLastCall();

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$set").add(GridFs.CHUNK_NUMBER_FIELD, 0);
        expect(
                mockChunks.update(eq(BuilderFactory.start().add("_id", chunkId)
                        .build()), eq(update.build()), eq(false), eq(false),
                        eq(Durability.ACK))).andReturn(1L);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                fixedCmdResult.build());

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(true);
        assertThat(results, is(Collections.singletonMap((Object) "id", Arrays
                .asList("MD5 sums do not match. File document contains "
                        + "'md5 : '08d6c05a21512a79a1dfeb9d2a8f262f'' and the "
                        + "filemd5 command produced 'md5 : '123456''.",
                        "File repaired."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testFsckMd5RepairButStillNotValid() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024)
                .add(GridFs.MD5_FIELD, "08d6c05a21512a79a1dfeb9d2a8f262f");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "123456");

        final DocumentBuilder fixedCmdResult = BuilderFactory.start();
        fixedCmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final ObjectId chunkId = new ObjectId();
        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.add("_id", chunkId)
                .addBinary("data", new byte[] { 1, 2, 3, 4 })
                .add(GridFs.CHUNK_NUMBER_FIELD, 0);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);
        final MongoIterator<Document> mockFileIterator = createMock(MongoIterator.class);
        final MongoIterator<Document> mockChunkIterator = createMock(MongoIterator.class);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        mockFiles.createIndex(true, Index.asc(GridFs.FILENAME_FIELD),
                Index.asc(GridFs.UPLOAD_DATE_FIELD));
        expectLastCall();
        mockChunks.createIndex(true, Index.asc(GridFs.FILES_ID_FIELD),
                Index.asc(GridFs.CHUNK_NUMBER_FIELD));
        expectLastCall();

        expect(mockFiles.find(Find.ALL)).andReturn(mockFileIterator);
        expect(mockFileIterator.iterator()).andReturn(mockFileIterator);
        expect(mockFileIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockFileIterator.next()).andReturn(fileResult.build());

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        expect(mockChunks.find(anyObject(Find.Builder.class))).andReturn(
                mockChunkIterator);
        expect(mockChunkIterator.iterator()).andReturn(mockChunkIterator);
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.TRUE);
        expect(mockChunkIterator.next()).andReturn(chunkResult.build());
        expect(mockChunkIterator.hasNext()).andReturn(Boolean.FALSE);
        mockChunkIterator.close();
        expectLastCall();

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$set").add(GridFs.CHUNK_NUMBER_FIELD, 0);
        expect(
                mockChunks.update(eq(BuilderFactory.start().add("_id", chunkId)
                        .build()), eq(update.build()), eq(false), eq(false),
                        eq(Durability.ACK))).andReturn(1L);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                fixedCmdResult.build());

        expect(mockFileIterator.hasNext()).andReturn(Boolean.FALSE);
        mockFileIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);

        final GridFs fs = new GridFs(mockDb);
        final Map<Object, List<String>> results = fs.fsck(true);
        assertThat(
                results,
                is(Collections.singletonMap(
                        (Object) "id",
                        Arrays.asList(
                                "MD5 sums do not match. File document contains "
                                        + "'md5 : '08d6c05a21512a79a1dfeb9d2a8f262f'' and the "
                                        + "filemd5 command produced 'md5 : '123456''.",
                                "Repair failed: Chunks reordered but sill not validating."))));

        verify(mockDb, mockFiles, mockChunks, mockFileIterator,
                mockChunkIterator);
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
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

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
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadChunkMissingBytes() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.add(GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
            fail("Read should have failed.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe.getMessage(),
                    containsString("Missing bytes in chunk"));
        }

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadMissingChunkNumber() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
            fail("Read should have failed.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe.getMessage(), containsString("Missing chunk number"));
        }

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadMissingChunks() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

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
        expect(mockIterator.hasNext()).andReturn(Boolean.FALSE);
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
            fail("Read should have failed.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe.getMessage(), containsString("Missing chunks"));
        }

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadMissingChunkSizeOnly() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

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
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadMissingMetadata() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

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
            fail("Read should have failed.");
        }
        catch (final FileNotFoundException fnfe) {
            // Excellent.
        }

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadObjectId() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        fs.read(new ObjectId(), sink);
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
    public void testReadObjectIdNotFound() throws IOException {
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
            fs.read(new ObjectId(), sink);
            fail("Read should have failed.");
        }
        catch (final FileNotFoundException fnfe) {
            // Excellent.
        }

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadOutOfOrderChunks() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3, 4 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 2);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
            fail("Read should have failed.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe.getMessage(), containsString("Skipped chunk"));
        }

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadWithZeroLengthFileAndEmptyChunk() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 0)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] {}).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        fs.read("foo", sink);
        assertArrayEquals(new byte[] {}, sink.toByteArray());

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadWrongSize() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 4)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

        final DocumentBuilder chunkResult = BuilderFactory.start();
        chunkResult.addBinary("data", new byte[] { 1, 2, 3 }).add(
                GridFs.CHUNK_NUMBER_FIELD, 0);

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
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        try {
            fs.read("foo", sink);
            fail("Read should have failed.");
        }
        catch (final IOException ioe) {
            // Good.
            assertThat(ioe.getMessage(), containsString("File size mismatch"));
        }
        assertArrayEquals(new byte[] { 1, 2, 3 }, sink.toByteArray());

        verify(mockDb, mockFiles, mockChunks, mockIterator);
    }

    /**
     * Test method for {@link GridFs#read}.
     * 
     * @throws IOException
     *             On an error.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReadZeroLengthFile() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.LENGTH_FIELD, 0)
                .add(GridFs.CHUNK_SIZE_FIELD, 1024);

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
        expect(mockIterator.hasNext()).andReturn(Boolean.FALSE);
        mockIterator.close();
        expectLastCall();

        replay(mockDb, mockFiles, mockChunks, mockIterator);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayOutputStream sink = new ByteArrayOutputStream(4);
        fs.read("foo", sink);
        assertArrayEquals(new byte[] {}, sink.toByteArray());

        verify(mockDb, mockFiles, mockChunks, mockIterator);
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
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkInterrupted() throws IOException {
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

        expect(mockFiles.deleteAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);

        try {
            Thread.currentThread().interrupt();
            assertFalse(fs.unlink("foo"));
        }
        finally {
            Thread.interrupted();
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
    public void testUnlinkObjectId() throws IOException {
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
        assertTrue(fs.unlink(new ObjectId()));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#unlink(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testUnlinkObjectidFailesOnFilesLookup() throws IOException {
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
        assertFalse(fs.unlink(new ObjectId()));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidate() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertTrue(fs.validate("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidateFails() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef1234");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.validate("foo"));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidateFileNotFound() throws IOException {

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(null);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        try {
            fs.validate("foo");
            fail("Validate should have failed.");
        }
        catch (final FileNotFoundException fnfe) {
            // Good.
            assertThat(fnfe.getMessage(), containsString("foo"));
        }

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(ObjectId)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidateObjectId() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id").add(GridFs.MD5_FIELD, "abcdef");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1).add(GridFs.MD5_FIELD, "abcdef");

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertTrue(fs.validate(new ObjectId()));

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidateObjectIdFileNotFound() throws IOException {

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(null);

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ObjectId id = new ObjectId();
        try {
            fs.validate(id);
            fail("Validate should have failed.");
        }
        catch (final FileNotFoundException fnfe) {
            // Good.
            assertThat(fnfe.getMessage(), containsString(id.toString()));
        }

        verify(mockDb, mockFiles, mockChunks);
    }

    /**
     * Test method for {@link GridFs#validate(String)}.
     * 
     * @throws IOException
     *             On an error.
     */
    @Test
    public void testValidateWithNoMd5InResults() throws IOException {
        final DocumentBuilder fileResult = BuilderFactory.start();
        fileResult.addString("_id", "id");

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.add("filemd5", "id");
        commandDoc.add("root", "fs");

        final DocumentBuilder cmdResult = BuilderFactory.start();
        cmdResult.add("ok", 1);

        final MongoDatabase mockDb = createMock(MongoDatabase.class);
        final MongoCollection mockFiles = createMock(MongoCollection.class);
        final MongoCollection mockChunks = createMock(MongoCollection.class);

        expect(mockFiles.findOne(anyObject(DocumentAssignable.class)))
                .andReturn(fileResult.build());

        expect(mockDb.getCollection("fs" + GridFs.FILES_SUFFIX)).andReturn(
                mockFiles);
        expect(mockDb.getCollection("fs" + GridFs.CHUNKS_SUFFIX)).andReturn(
                mockChunks);

        expect(mockDb.runCommand(commandDoc.build())).andReturn(
                cmdResult.build());

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        assertFalse(fs.validate("foo"));

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
    @SuppressWarnings("boxing")
    @Test
    public void testWriteInterrupted() throws IOException {
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

        expect(mockFiles.insertAsync(anyObject(DocumentAssignable.class)))
                .andReturn(futureFiles);

        expect(mockChunks.delete(anyObject(DocumentAssignable.class)))
                .andReturn(0L);
        expect(mockFiles.delete(anyObject(DocumentAssignable.class)))
                .andReturn(0L);

        replay(mockDb, mockFiles, mockChunks);

        final GridFs fs = new GridFs(mockDb);
        final ByteArrayInputStream in = new ByteArrayInputStream(
                new byte[fs.getChunkSize()]);
        try {
            Thread.currentThread().interrupt();
            fs.write("foo", in);
            fail("Should have thrown an exception.");
        }
        catch (final InterruptedIOException error) {
            // Good.
            assertTrue(error.getCause() instanceof InterruptedException);
        }
        finally {
            Thread.interrupted();
        }
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
