/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.VersionRange;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * CommandTest provides tests for the {@link Command} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CommandTest {

    /**
     * Test the streaming of Commands.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testBsonWrite() throws IOException {
        final Command command = new Command("db", BuilderFactory.start()
                .build());

        final ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut1 = new BsonOutputStream(out1);
        command.write(1000, bsonOut1);

        final ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        final BufferingBsonOutputStream bsonOut2 = new BufferingBsonOutputStream(
                out2);
        command.write(1000, bsonOut2);

        assertArrayEquals(out1.toByteArray(), out2.toByteArray());
    }

    /**
     * Test the streaming of Commands.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testBsonWriteWithSecondaryOkReadPreference() throws IOException {
        final Command command = new Command("db", BuilderFactory.start()
                .build(), ReadPreference.PREFER_SECONDARY);

        final ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        final BsonOutputStream bsonOut1 = new BsonOutputStream(out1);
        command.write(1000, bsonOut1);

        final ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        final BufferingBsonOutputStream bsonOut2 = new BufferingBsonOutputStream(
                out2);
        command.write(1000, bsonOut2);

        assertArrayEquals(out1.toByteArray(), out2.toByteArray());
    }

    /**
     * Test method for {@link AggregateCommand#equals(Object)} and
     * {@link AggregateCommand#hashCode()} .
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {
        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();

        // Not in the equals or has code.
        final VersionRange range = VersionRange.minimum(Version.VERSION_2_6);

        for (final Document document : Arrays.asList(doc1, doc2, doc3)) {
            for (final String databaseName : Arrays.asList("n1", "n2", "n3")) {
                for (final ReadPreference readPreference : Arrays
                        .asList(ReadPreference.PRIMARY)) {

                    objs1.add(new Command(databaseName, document,
                            readPreference, range));
                    objs2.add(new Command(databaseName, document,
                            readPreference, range));
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Message obj1 = objs1.get(i);
            Message obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new IsMaster()));
        }
    }

    /**
     * Test method for {@link Command#getOperationName()}.
     */
    @Test
    public void testGetOperationNameWithEmptyCommandDocument() {
        final Command command = new Command("db", BuilderFactory.start()
                .build());

        assertThat(command.getOperationName(), is("command"));
    }

    /**
     * Test the stringify of a command.
     */
    @Test
    public void testToString() throws IOException {
        final Command command = new Command("db", BuilderFactory.start()
                .add("foo", 1).build());

        assertThat(command.toString(),
                is("Command[foo, db=db, collection=$cmd, "
                        + "readPreference=PRIMARY_ONLY]: { foo : 1 }"));
    }

    /**
     * Test the stringify of a command.
     */
    @Test
    public void testToStringNoReadPreference() throws IOException {
        final Command command = new Command("db", BuilderFactory.start()
                .add("foo", 1).build(), null);

        assertThat(command.toString(),
                is("Command[foo, db=db, collection=$cmd]: { foo : 1 }"));
    }

    /**
     * Test the stringify of a command.
     */
    @Test
    public void testToStringWithVersionRange() throws IOException {
        final Command command = new Command("db", BuilderFactory.start()
                .add("foo", 1).build(), ReadPreference.PRIMARY,
                VersionRange.range(Version.VERSION_2_2, Version.VERSION_2_4));

        assertThat(command.toString(),
                is("Command[foo, db=db, collection=$cmd, "
                        + "readPreference=PRIMARY_ONLY, "
                        + "requiredVersionRange=[2.2, 2.4)]: { foo : 1 }"));
    }

    /**
     * Test the validation of the size of the command.
     */
    @Test
    public void testValidateSize() throws IOException {
        final SizeOfVisitor visitor = new SizeOfVisitor();

        final Command command = new Command("db", BuilderFactory.start()
                .add("foo", 1).build(), ReadPreference.PRIMARY,
                VersionRange.range(Version.VERSION_2_2, Version.VERSION_2_4));

        try {
            command.validateSize(visitor, 1);
            fail("Should have thrown a DocumentToLargeException.");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(command.getCommand()));
            assertThat(error.getMaximumSize(), is(1));
            assertThat(error.getSize(), is((int) command.getCommand().size()));
        }

        // Should not throw.
        command.validateSize(visitor, 1000000);

        // Should not throw if Jumbo either
        command.setAllowJumbo(true);
        command.validateSize(visitor, 1);

        final Command bigCommand = new Command("db", BuilderFactory.start()
                .add("foo", new byte[16 * 1024]).build(),
                ReadPreference.PRIMARY, VersionRange.range(Version.VERSION_2_2,
                        Version.VERSION_2_4));
        // Should throw if Jumbo still too big
        try {
            bigCommand.setAllowJumbo(true);
            bigCommand.validateSize(visitor, 1);
            fail("Should have thrown a DocumentToLargeException.");
        }
        catch (final DocumentToLargeException error) {
            assertThat(error.getDocument(), is(bigCommand.getCommand()));
            assertThat(error.getMaximumSize(), is((16 * 1024) + 1));
            assertThat(error.getSize(),
                    is((int) bigCommand.getCommand().size() + 4));
        }

    }
}
