/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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

/**
 * DurabilityTest provides tests for the {@link Durability} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DurabilityTest {

    /**
     * Test method for
     * {@link com.allanbank.mongodb.Durability#equals(java.lang.Object)}.
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
     * Test method for
     * {@link com.allanbank.mongodb.Durability#fsyncDurable(int)}.
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
     * Test method for
     * {@link com.allanbank.mongodb.Durability#journalDurable(int)}.
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
     * Test method for {@link com.allanbank.mongodb.Durability#readResolve} .
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
     * Test method for
     * {@link com.allanbank.mongodb.Durability#replicaDurable(boolean, int, int)}
     * .
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
        assertEquals(journal, durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(replicaCount, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.Durability#replicaDurable(boolean, String, int)}
     * .
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
        assertEquals(journal, durability.isWaitForJournal());
        assertTrue(durability.isWaitForReply());
        assertEquals(wait, durability.getWaitTimeoutMillis());
        assertEquals(0, durability.getWaitForReplicas());
        assertEquals(tag, durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.Durability#replicaDurable(int)}.
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
        assertEquals(1, durability.getWaitForReplicas());
        assertNull(durability.getWaitForReplicasByMode());
        assertNull(durability.getWaitForReplicasByMode());
    }

    /**
     * Test method for
     * {@link com.allanbank.mongodb.Durability#replicaDurable(int, int)}.
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
     * Test method for
     * {@link com.allanbank.mongodb.Durability#replicaDurable(String, int)}.
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
    }
}
