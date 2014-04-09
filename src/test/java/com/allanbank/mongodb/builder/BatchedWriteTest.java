/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.a;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.write.DeleteOperation;
import com.allanbank.mongodb.builder.write.InsertOperation;
import com.allanbank.mongodb.builder.write.UpdateOperation;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * BatchedWriteTest provides tests for the {@link BatchedWrite} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteTest {

    /**
     * Test method for {@link BatchedWrite#getDurability()}.
     */
    @Test
    public void testGetDurability() {
        final Document doc = d().build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.insert(doc);
        builder.update(doc, doc);
        builder.delete(doc);

        // Default is null.
        assertThat(builder.build().getDurability(), nullValue());

        for (final Durability durability : Arrays.asList(Durability.ACK,
                Durability.NONE, Durability.journalDurable(100), null)) {

            builder.durability(durability);

            final BatchedWrite write = builder.build();
            assertThat(write.getDurability(), is(durability));
        }
    }

    /**
     * Test method for {@link BatchedWrite#getMode()}.
     */
    @Test
    public void testGetMode() {
        final Document doc = d().build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.insert(doc);
        builder.update(doc, doc);
        builder.delete(doc);

        // Default is BatchedWriteMode.SERIALIZE_AND_CONTINUE.
        assertThat(builder.build().getMode(),
                is(BatchedWriteMode.SERIALIZE_AND_CONTINUE));

        for (final BatchedWriteMode mode : BatchedWriteMode.values()) {

            builder.mode(mode);

            final BatchedWrite write = builder.build();
            assertThat(write.getMode(), is(mode));
        }
    }

    /**
     * Test method for {@link BatchedWrite#getWrites()}.
     */
    @Test
    public void testGetWrites() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc);
        builder.save(doc);
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);

        final BatchedWrite write = builder.build();
        final List<WriteOperation> writes = write.getWrites();
        assertThat(writes, hasSize(9));

        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));

        assertThat(writes.get(1),
                is((WriteOperation) new UpdateOperation(d(doc.get("_id")), doc,
                        false, true)));

        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(5), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));

        assertThat(writes.get(6), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(7), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(8), is((WriteOperation) new DeleteOperation(
                query, true)));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundles() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(3));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(5));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true)),
                                        d(e("q", query), e("u", update),
                                                e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(3));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(2), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesALot() {

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        for (int i = 0; i < 100000; ++i) {
            builder.insert(d().build());
        }

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000000);

        assertThat(bundles, hasSize(1));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesBreakBatchForSize() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        // Force only two operations per batch.
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo", 100,
                1000);

        assertThat(bundles, hasSize(9));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(
                bundle.getCommand(),
                is(d(e("update", "foo"), e("ordered", false),
                        e("updates", a(d(e("q", query), e("u", update)))))
                        .build()));

        bundle = bundles.get(3);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(
                bundle.getCommand(),
                is(d(e("update", "foo"), e("ordered", false),
                        e("updates", a(d(e("q", query), e("u", update)))))
                        .build()));

        bundle = bundles.get(4);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update),
                                        e("multi", true))))).build()));

        bundle = bundles.get(5);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update),
                                        e("upsert", true))))).build()));

        bundle = bundles.get(6);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(
                bundle.getCommand(),
                is(d(e("delete", "foo"), e("ordered", false),
                        e("deletes", a(d(e("q", query), e("limit", 0)))))
                        .build()));

        bundle = bundles.get(7);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(
                bundle.getCommand(),
                is(d(e("delete", "foo"), e("ordered", false),
                        e("deletes", a(d(e("q", query), e("limit", 0)))))
                        .build()));

        bundle = bundles.get(8);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(e("delete", "foo"), e("ordered", false),
                        e("deletes", a(d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesDeleteTooBig() {
        final Document query = d(e("a", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.delete(query);

        final BatchedWrite write = builder.build();

        try {
            write.toBundles("foo", query.size() - 10, 1000);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(query));
            assertThat(error.getMaximumSize(), is((int) query.size() - 10));
            assertThat(error.getSize(), greaterThan((int) (query.size())));
        }
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesInsertTooBig() {
        final Document doc = d().build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.insert(doc); // Insert - no id yet.

        final BatchedWrite write = builder.build();

        try {
            write.toBundles("foo", doc.size() - 10, 1000);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(doc));
            assertThat(error.getMaximumSize(), is((int) doc.size() - 10));
            assertThat(error.getSize(), is((int) doc.size()));
        }
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesJournaled() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);
        builder.durability(Durability.journalDurable(1000));

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(3));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("insert", "foo"),
                        e("ordered", false),
                        e("writeConcern", d(e("j", true), e("wtimeout", 1000))),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(5));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("writeConcern", d(e("j", true), e("wtimeout", 1000))),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true)),
                                        d(e("q", query), e("u", update),
                                                e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(3));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(2), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("writeConcern", d(e("j", true), e("wtimeout", 1000))),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesManuallyCreatedWrites() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);

        final List<WriteOperation> operations = builder.build().getWrites();
        builder.writes(null); // Clears the writes.
        builder.writes(operations.subList(0, operations.size() - 1));
        builder.write(operations.get(operations.size() - 1));

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(3));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(5));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true)),
                                        d(e("q", query), e("u", update),
                                                e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(3));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(2), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesOnlyTwoOperationsPerBatch() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        // Force only two operations per batch.
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 2);

        assertThat(bundles, hasSize(6));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)))))
                        .build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true))))).build()));

        bundle = bundles.get(3);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update),
                                        e("upsert", true))))).build()));

        bundle = bundles.get(4);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)))))
                        .build()));

        bundle = bundles.get(5);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(e("delete", "foo"), e("ordered", false),
                        e("deletes", a(d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesOptimized() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);
        builder.mode(BatchedWriteMode.REORDERED);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(3));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(5));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true)),
                                        d(e("q", query), e("u", update),
                                                e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(3));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(2), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesOptimizedOnlyTwoOperationsPerBatch() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);
        builder.mode(BatchedWriteMode.REORDERED);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        // Force only two operations per batch.
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 2);

        assertThat(bundles, hasSize(6));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(
                bundle.getCommand(),
                is(d(e("insert", "foo"), e("ordered", false),
                        e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)))))
                        .build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true))))).build()));

        bundle = bundles.get(3);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("ordered", false),
                        e("updates",
                                a(d(e("q", query), e("u", update),
                                        e("upsert", true))))).build()));

        bundle = bundles.get(4);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(2));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("ordered", false),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)))))
                        .build()));

        bundle = bundles.get(5);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(e("delete", "foo"), e("ordered", false),
                        e("deletes", a(d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesOptimizedTooBig() {
        final Document doc = d().build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.insert(doc); // Insert - no id yet.
        builder.mode(BatchedWriteMode.REORDERED);

        final BatchedWrite write = builder.build();

        try {
            write.toBundles("foo", doc.size() - 10, 1000);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(doc));
            assertThat(error.getMaximumSize(), is((int) doc.size() - 10));
            assertThat(error.getSize(), is((int) doc.size()));
        }
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesOptimizedWithLotsAndLotsOfOperations() {
        final int count = 100000;

        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();
        final DocumentBuilder doc = BuilderFactory.start();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        for (int i = 0; i < count; ++i) {
            builder.insert(doc);
            builder.update(query, update);
            builder.update(query, update, false, false);
            builder.update(query, update, true, false);
            builder.update(query, update, false, true);
            builder.delete(query);
            builder.delete(query, false);
            builder.delete(query, true);
        }
        builder.mode(BatchedWriteMode.REORDERED);

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(8 * (count / 1000)));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesSerializeAndStop() {
        final Document doc = d().build();
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.save(doc); // Insert - no id yet.
        builder.save(doc); // Update - has an id now.
        builder.update(query, update);
        builder.update(query, update, false, false);
        builder.update(query, update, true, false);
        builder.update(query, update, false, true);
        builder.delete(query);
        builder.delete(query, false);
        builder.delete(query, true);
        builder.mode(BatchedWriteMode.SERIALIZE_AND_STOP);

        final Element id = doc.get("_id");

        final BatchedWrite write = builder.build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles("foo",
                Client.MAX_DOCUMENT_SIZE, 1000);

        assertThat(bundles, hasSize(3));

        BatchedWrite.Bundle bundle = bundles.get(0);
        List<WriteOperation> writes = bundle.getWrites();
        assertThat(writes, hasSize(1));
        assertThat(writes.get(0), is((WriteOperation) new InsertOperation(doc)));
        assertThat(bundle.getCommand(),
                is(d(e("insert", "foo"), e("documents", a(doc))).build()));

        bundle = bundles.get(1);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(5));
        assertThat(writes.get(0), is((WriteOperation) new UpdateOperation(
                d(id), doc, false, true)));
        assertThat(writes.get(1), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(2), is((WriteOperation) new UpdateOperation(
                query, update, false, false)));
        assertThat(writes.get(3), is((WriteOperation) new UpdateOperation(
                query, update, true, false)));
        assertThat(writes.get(4), is((WriteOperation) new UpdateOperation(
                query, update, false, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("update", "foo"),
                        e("updates",
                                a(d(e("q", d(id)), e("u", doc),
                                        e("upsert", true)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update)),
                                        d(e("q", query), e("u", update),
                                                e("multi", true)),
                                        d(e("q", query), e("u", update),
                                                e("upsert", true))))).build()));

        bundle = bundles.get(2);
        writes = bundle.getWrites();
        assertThat(writes, hasSize(3));
        assertThat(writes.get(0), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(1), is((WriteOperation) new DeleteOperation(
                query, false)));
        assertThat(writes.get(2), is((WriteOperation) new DeleteOperation(
                query, true)));
        assertThat(
                bundle.getCommand(),
                is(d(
                        e("delete", "foo"),
                        e("deletes",
                                a(d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 0)),
                                        d(e("q", query), e("limit", 1)))))
                        .build()));
    }

    /**
     * Test method for {@link BatchedWrite#toBundles(String, long, int)}.
     */
    @Test
    public void testToBundlesUpdateTooBig() {
        final Document query = d(e("a", 1)).build();
        final Document update = d(e("b", 1)).build();

        final BatchedWrite.Builder builder = BatchedWrite.builder();
        builder.update(query, update);

        final BatchedWrite write = builder.build();

        try {
            write.toBundles("foo", query.size() - 10, 1000);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(query));
            assertThat(error.getMaximumSize(), is((int) query.size() - 10));
            assertThat(error.getSize(),
                    greaterThan((int) (query.size() + update.size())));
        }
    }

}
