/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.client.connection.auth.MongoDbAuthenticator;

/**
 * CredentialTest provides tests for the {@link Credential} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CredentialTest {

    /**
     * Test method for {@link Credential#authenticator()}.
     */
    @Test
    public void testAuthenticator() {
        final MongoDbAuthenticator auth = new MongoDbAuthenticator();

        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database("foo").authenticator(auth)
                .build();

        assertEquals("user", c.getUserName());
        assertArrayEquals(new char[1], c.getPassword());
        assertEquals("foo", c.getDatabase());
        assertEquals(MongoDbAuthenticator.class.getName(),
                c.getAuthenticationType());
        assertSame(auth, c.getAuthenticator());
        assertNotSame(auth, c.authenticator());
        assertTrue(c.authenticator() instanceof MongoDbAuthenticator);
    }

    /**
     * Test method for {@link Credential#authenticator()}.
     */
    @Test(expected = MongoDbException.class)
    public void testAuthenticatorFails() {
        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database("foo")
                .authenticationType("fail").build();

        c.authenticator();
    }

    /**
     * Test method for {@link Credential#authenticator()}.
     */
    @Test
    public void testDefaultDB() {
        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database(null)
                .authenticationType("fail").build();

        assertEquals(Credential.ADMIN_DB, c.getDatabase());
    }

    /**
     * Test method for {@link Credential#equals(Object)}.
     */
    @Test
    public void testEqualsObject() {

        final List<Credential> objs1 = new ArrayList<Credential>();
        final List<Credential> objs2 = new ArrayList<Credential>();

        final File file1 = new File("a");
        final File file2 = new File("b");

        for (final String user : Arrays.asList("a", "b", "c", null)) {
            for (final String passwd : Arrays.asList("a", "b", "c", null)) {
                for (final String db : Arrays.asList("a", "b", "c", null)) {
                    for (final File file : Arrays.asList(file1, file2, null)) {
                        for (final String type : Arrays.asList("a", "b", "c",
                                null)) {
                            if (passwd == null) {
                                objs1.add(Credential.builder().userName(user)
                                        .password(null).database(db).file(file)
                                        .authenticationType(type).build());
                                objs2.add(Credential.builder().userName(user)
                                        .password(null).database(db).file(file)
                                        .authenticationType(type).build());

                            }
                            else {
                                objs1.add(Credential.builder().userName(user)
                                        .password(passwd.toCharArray())
                                        .database(db).file(file)
                                        .authenticationType(type).build());
                                objs2.add(Credential.builder().userName(user)
                                        .password(passwd.toCharArray())
                                        .database(db).file(file)
                                        .authenticationType(type).build());

                                objs1.add(Credential.builder().userName(user)
                                        .password(passwd.toCharArray())
                                        .database(db).file(file)
                                        .addOption("f", "g")
                                        .authenticationType(type).build());
                                objs2.add(Credential.builder().userName(user)
                                        .addOption("f", "g")
                                        .password(passwd.toCharArray())
                                        .database(db).file(file)
                                        .authenticationType(type).build());
                            }
                        }
                    }
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Credential obj1 = objs1.get(i);
            Credential obj2 = objs2.get(i);

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
     * Test method for {@link Credential#getOption(String, int)}.
     */
    @Test
    public void testGetOptionBoolean() {
        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database(null).addOption("f", true)
                .addOption("h", "True").authenticationType("fail").build();

        assertThat(c.getOption("f", false), is(true));
        assertThat(c.getOption("h", false), is(true));
        assertThat(c.getOption("j", false), is(false));
    }

    /**
     * Test method for {@link Credential#getOption(String, int)}.
     */
    @Test
    public void testGetOptionInt() {
        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database(null).addOption("f", 1)
                .addOption("h", "i").authenticationType("fail").build();

        assertThat(c.getOption("f", 10), is(1));
        assertThat(c.getOption("h", 10), is(10));
        assertThat(c.getOption("j", 10), is(10));
    }

    /**
     * Test method for {@link Credential#getOption(String, String)}.
     */
    @Test
    public void testGetOptionString() {
        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database(null).addOption("f", "g")
                .addOption("h", "i").authenticationType("fail").build();

        assertThat(c.getOption("f", "other"), is("g"));
        assertThat(c.getOption("h", "other"), is("i"));
        assertThat(c.getOption("j", "other"), is("other"));
    }

    /**
     * Test method for {@link Credential#hasPassword()}.
     */
    @Test
    public void testHasPassword() {
        Credential c = Credential.builder().userName("user")
                .password(new char[1]).database(null)
                .authenticationType("fail").build();
        assertTrue(c.hasPassword());

        c = Credential.builder().userName("user").password(new char[0])
                .database(null).authenticationType("fail").build();
        assertFalse(c.hasPassword());
    }

    /**
     * Test method for {@link Credential#authenticator()}.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws ClassNotFoundException
     *             On a test failure.
     */
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        final MongoDbAuthenticator auth = new MongoDbAuthenticator();

        final Credential c = Credential.builder().userName("user")
                .password(new char[1]).database("foo").authenticator(auth)
                .build();

        assertEquals("user", c.getUserName());
        assertArrayEquals(new char[1], c.getPassword());
        assertEquals("foo", c.getDatabase());
        assertEquals(MongoDbAuthenticator.class.getName(),
                c.getAuthenticationType());
        assertSame(auth, c.getAuthenticator());
        assertNotSame(auth, c.authenticator());
        assertTrue(c.authenticator() instanceof MongoDbAuthenticator);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oout = new ObjectOutputStream(out);

        oout.writeObject(c);
        oout.flush();

        final ObjectInputStream oin = new ObjectInputStream(
                new ByteArrayInputStream(out.toByteArray()));

        final Object obj = oin.readObject();
        assertThat(obj, instanceOf(Credential.class));

        final Credential other = (Credential) obj;
        assertThat(other, is(c));

        assertEquals("user", other.getUserName());
        assertArrayEquals(new char[1], other.getPassword());
        assertEquals("foo", other.getDatabase());
        assertEquals(MongoDbAuthenticator.class.getName(),
                other.getAuthenticationType());
        assertNull(other.getAuthenticator());
        assertNotSame(auth, other.authenticator());
        assertTrue(c.authenticator() instanceof MongoDbAuthenticator);
    }

    /**
     * Test method for {@link Credential#toString()}.
     */
    @Test
    public void testToString() {
        final MongoDbAuthenticator auth = new MongoDbAuthenticator();
        Credential c = Credential.builder().userName("user")
                .password(new char[1]).database("foo").authenticator(auth)
                .build();

        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'MONGODB-CR' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .database("foo").kerberos().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'KERBEROS' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .database("foo").mongodbCR().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'MONGODB-CR' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .database("foo").mongodbCR().addOption("f", "true").build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'MONGODB-CR', 'f': 'true' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .database("foo").authenticationType("bar").build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'bar' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .file(new File("a")).database("foo").kerberos().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', file : 'a', password : '<redacted>', type: 'KERBEROS' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .file(new File("a")).database("foo").x509().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', file : 'a', password : '<redacted>', type: 'x.509' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .file(new File("a")).database("foo").plainSasl().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', file : 'a', password : '<redacted>', type: 'PLAIN SASL' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .file(new File("a")).database("foo").ldap().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', file : 'a', password : '<redacted>', type: 'PLAIN SASL' }"));

        c = Credential.builder().userName("user").password(new char[1])
                .file(new File("a")).database("foo").pam().build();
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', file : 'a', password : '<redacted>', type: 'PLAIN SASL' }"));
    }
}
