/*
 * #%L
 * DurabilityTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;

/**
 * DurabilityTest provides tests for the {@link Durability} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DurabilityTest {

    /**
     * Test method for {@link Durability#asDocument()}.
     */
    @Test
    public void testAsDocument() {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("getlasterror", 1);
        // This is really bogus but reasonable
        assertEquals(builder.asDocument(), Durability.NONE.asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("w", 1);
        assertEquals(builder.asDocument(), Durability.ACK.asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("fsync", true);
        builder.add("wtimeout", 123);
        assertEquals(builder.asDocument(), Durability.fsyncDurable(123)
                .asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("j", true);
        builder.add("wtimeout", 124);
        assertEquals(builder.asDocument(), Durability.journalDurable(124)
                .asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("wtimeout", 125);
        builder.add("w", 2);
        assertEquals(builder.asDocument(), Durability.replicaDurable(125)
                .asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("wtimeout", 126);
        builder.add("w", 3);
        assertEquals(builder.asDocument(), Durability.replicaDurable(3, 126)
                .asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("wtimeout", 127);
        builder.add("w", "foo");
        assertEquals(builder.asDocument(), Durability
                .replicaDurable("foo", 127).asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("j", true);
        builder.add("wtimeout", 128);
        builder.add("w", "bar");
        assertEquals(builder.asDocument(),
                Durability.replicaDurable(true, "bar", 128).asDocument());

        builder.reset();
        builder.add("getlasterror", 1);
        builder.add("j", true);
        builder.add("wtimeout", 129);
        builder.add("w", 4);
        assertEquals(builder.asDocument(),
                Durability.replicaDurable(true, 4, 129).asDocument());

        // Second call should return the same document.
        final Durability durability = Durability.replicaDurable(true, 4, 129);
        assertThat(durability.asDocument(),
                sameInstance(durability.asDocument()));
        assertThat(durability.asDocument(), instanceOf(ImmutableDocument.class));
    }

    /**
     * Test method for {@link Durability#equals(java.lang.Object)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {

        final List<Durability> objs1 = new ArrayList<Durability>();
        final List<Durability> objs2 = new ArrayList<Durability>();

        objs1.add(Durability.NONE);
        objs2.add(Durability.NONE);

        objs1.add(Durability.ACK);
        objs2.add(Durability.ACK);

        for (final Boolean waitForFsync : Arrays.asList(Boolean.TRUE,
                Boolean.FALSE)) {
            for (final Boolean waitForJournal : Arrays.asList(Boolean.TRUE,
                    Boolean.FALSE)) {
                for (final Integer waitTimeoutMillis : Arrays.asList(1, 2, 3,
                        4, 10, 100, -1)) {
                    for (final Integer waitForReplicas : Arrays.asList(1, 2, 3,
                            4, 10, 100, -1)) {
                        objs1.add(new Durability(waitForFsync, waitForJournal,
                                waitForReplicas, waitTimeoutMillis));
                        objs2.add(new Durability(waitForFsync, waitForJournal,
                                waitForReplicas, waitTimeoutMillis));
                    }

                    for (final String mode : Arrays.asList("a", "b")) {
                        objs1.add(new Durability(waitForFsync, waitForJournal,
                                mode, waitTimeoutMillis));
                        objs2.add(new Durability(waitForFsync, waitForJournal,
                                mode, waitTimeoutMillis));
                    }
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Durability obj1 = objs1.get(i);
            Durability obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }

    /**
     * Test method for {@link Durability#fsyncDurable(int)}.
     */
    @Test
    public void testFsyncDurable() {
        final Random random = new Random(System.currentTimeMillis());
        final int wait = random.nextInt(100000);

        final Durability durability = Durability.fsyncDurable(wait);

        assertTrue(durability.isWaitForFsync());
        assertFalse(durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(0, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#journalDurable(int)}.
     */
    @Test
    public void testJournalDurable() {
        final Random random = new Random(System.currentTimeMillis());
        final int wait = random.nextInt(100000);

        final Durability durability = Durability.journalDurable(wait);

        assertFalse(durability.isWaitForFsync());
        assertTrue(durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(0, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#readResolve} .
     * 
     * @throws IOException
     *             On a failure.
     * @throws ClassNotFoundException
     *             On a failure.
     */
    @Test
    public void testReadResolve() throws IOException, ClassNotFoundException {
        for (final Durability d : Arrays
                .asList(Durability.ACK, Durability.NONE)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final ObjectOutputStream oout = new ObjectOutputStream(out);
            oout.writeObject(d);
            oout.close();

            final ByteArrayInputStream in = new ByteArrayInputStream(
                    out.toByteArray());
            final ObjectInputStream oin = new ObjectInputStream(in);

            assertSame(d, oin.readObject());
        }

        final Durability d = Durability.journalDurable(100);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(out);
        oout.writeObject(d);
        oout.close();

        final ByteArrayInputStream in = new ByteArrayInputStream(
                out.toByteArray());
        final ObjectInputStream oin = new ObjectInputStream(in);

        final Object read = oin.readObject();
        assertEquals(d, read);
        assertFalse(d == read);
    }

    /**
     * Test method for {@link Durability#replicaDurable(boolean, int, int)} .
     */
    @Test
    public void testReplicaDurableBooleanIntInt() {
        final Random random = new Random(System.currentTimeMillis());
        final boolean journal = random.nextBoolean();
        final int wait = random.nextInt(100000);
        final int replicaCount = random.nextInt(10000);

        final Durability durability = Durability.replicaDurable(journal,
                replicaCount, wait);

        assertFalse(durability.isWaitForFsync());
        assertEquals(Boolean.valueOf(journal),
                Boolean.valueOf(durability.isWaitForJournal()));
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(replicaCount, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#replicaDurable(boolean, String, int)} .
     */
    @Test
    public void testReplicaDurableBooleanStringInt() {
        final Random random = new Random(System.currentTimeMillis());
        final boolean journal = random.nextBoolean();
        final int wait = random.nextInt(100000);
        final String tag = String.valueOf(random.nextInt(10000));

        final Durability durability = Durability.replicaDurable(journal, tag,
                wait);

        assertFalse(durability.isWaitForFsync());
        assertEquals(Boolean.valueOf(journal),
                Boolean.valueOf(durability.isWaitForJournal()));
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(0, durability.getWaitForReplicas());
        assertEquals(tag, durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#replicaDurable(int)}.
     */
    @Test
    public void testReplicaDurableInt() {
        final Random random = new Random(System.currentTimeMillis());
        final int wait = random.nextInt(100000);

        final Durability durability = Durability.replicaDurable(wait);

        assertFalse(durability.isWaitForFsync());
        assertFalse(durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(2, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#replicaDurable(int, int)}.
     */
    @Test
    public void testReplicaDurableIntInt() {
        final Random random = new Random(System.currentTimeMillis());
        final int wait = random.nextInt(100000);
        final int replicaCount = random.nextInt(10000);

        final Durability durability = Durability.replicaDurable(replicaCount,
                wait);

        assertFalse(durability.isWaitForFsync());
        assertFalse(durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(replicaCount, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for {@link Durability#replicaDurable(String, int)}.
     */
    @Test
    public void testReplicaDurableStringInt() {
        final Random random = new Random(System.currentTimeMillis());
        final int wait = random.nextInt(100000);
        final String tag = String.valueOf(random.nextInt(10000));

        final Durability durability = Durability.replicaDurable(tag, wait);

        assertFalse(durability.isWaitForFsync());
        assertFalse(durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(0, durability.getWaitForReplicas());
        assertEquals(tag, durability.getWaitForReplicasByMode());

        assertEquals("{ getlasterror : 1, wtimeout : " + wait + ", w : '" + tag
                + "' }", durability.toString());
    }

    /**
     * Test method for {@link Durability#valueOf(String)}.
     */
    @Test
    public void testValueOf() {

        assertSame(Durability.ACK, Durability.valueOf("AcK"));
        assertSame(Durability.ACK, Durability.valueOf("sAfe"));
        assertSame(Durability.NONE, Durability.valueOf("NoNe"));

        assertEquals(Durability.replicaDurable(true, 4, 129),
                Durability.valueOf("{ j : true, wtimeout : 129, w : 4 }"));
        assertEquals(
                Durability.replicaDurable(true, "bar", 128),
                Durability
                        .valueOf("{ getlasterror : 1, j : true, wtimeout : 128, w : 'bar' }"));
        assertEquals(
                Durability.replicaDurable(true, "bar", 128),
                Durability
                        .valueOf("{ getlasterror : 1, j : 1, wtimeout : 128, w : bar }"));
        assertEquals(
                Durability.replicaDurable("foo", 127),
                Durability
                        .valueOf("{ getlasterror : 1, wtimeout : 127, w : 'foo' }"));
        assertEquals(Durability.replicaDurable(3, 126),
                Durability
                        .valueOf("{ getlasterror : 1, wtimeout : 126, w : 3 }"));
        assertEquals(Durability.replicaDurable(125),
                Durability.valueOf("{ wtimeout : 125, w : 2 }"));
        assertEquals(Durability.journalDurable(124),
                Durability.valueOf("{ wtimeout : 124, j : 1 }"));
        assertEquals(Durability.fsyncDurable(123),
                Durability.valueOf("{ wtimeout : 123, fsync : 1 }"));

        assertNull(Durability.valueOf("{ wtimeout : 'a', fsync : 1 }"));
        assertNull(Durability.valueOf("{ foo : 1 }"));
        assertNull(Durability.valueOf("foo"));
    }
}
