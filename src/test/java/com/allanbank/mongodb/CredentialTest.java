/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.connection.auth.MongoDbAuthenticator;

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
        final Credential c = new Credential("user", new char[1], "foo", auth);

        assertEquals("user", c.getUsername());
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
        final Credential c = new Credential("user", new char[1], "foo", "fail");

        c.authenticator();
    }

    /**
     * Test method for {@link Credential#Credential(String, char[], String)} .
     */
    @Test
    public void testCredentialStringCharArrayString() {
        final Credential c = new Credential("user", new char[1], "foo");

        assertEquals("user", c.getUsername());
        assertArrayEquals(new char[1], c.getPassword());
        assertEquals(Credential.ADMIN_DB, c.getDatabase());
        assertEquals("foo", c.getAuthenticationType());
    }

    /**
     * Test method for
     * {@link Credential#Credential(String, char[], String, com.allanbank.mongodb.connection.auth.Authenticator)}
     * .
     */
    @Test
    public void testCredentialStringCharArrayStringAuthenticator() {
        final MongoDbAuthenticator auth = new MongoDbAuthenticator();
        final Credential c = new Credential("user", new char[1], "foo", auth);

        assertEquals("user", c.getUsername());
        assertArrayEquals(new char[1], c.getPassword());
        assertEquals("foo", c.getDatabase());
        assertEquals(MongoDbAuthenticator.class.getName(),
                c.getAuthenticationType());
        assertSame(auth, c.getAuthenticator());
    }

    /**
     * Test method for
     * {@link Credential#Credential(String, char[], String, String)} .
     */
    @Test
    public void testCredentialStringCharArrayStringString() {
        final Credential c = new Credential("user", new char[1], "foo", "auth");

        assertEquals("user", c.getUsername());
        assertArrayEquals(new char[1], c.getPassword());
        assertEquals("foo", c.getDatabase());
        assertEquals("auth", c.getAuthenticationType());
    }

    /**
     * Test method for {@link Credential#equals(Object)}.
     */
    @Test
    public void testEqualsObject() {

        final List<Credential> objs1 = new ArrayList<Credential>();
        final List<Credential> objs2 = new ArrayList<Credential>();

        for (final String user : Arrays.asList("a", "b", "c", null)) {
            for (final String passwd : Arrays.asList("a", "b", "c", null)) {
                for (final String db : Arrays.asList("a", "b", "c", null)) {
                    for (final String type : Arrays.asList("a", "b", "c", null)) {
                        if (passwd == null) {
                            objs1.add(new Credential(user, null, db, type));
                            objs2.add(new Credential(user, null, db, type));

                        }
                        else {
                            objs1.add(new Credential(user,
                                    passwd.toCharArray(), db, type));
                            objs2.add(new Credential(user,
                                    passwd.toCharArray(), db, type));
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
     * Test method for {@link Credential#toString()}.
     */
    @Test
    public void testToString() {
        final MongoDbAuthenticator auth = new MongoDbAuthenticator();
        Credential c = new Credential("user", new char[1], "foo", auth);

        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'MONGODB-CR' }"));

        c = new Credential("user", new char[1], "foo", Credential.KERBEROS);
        assertThat(
                c.toString(),
                is("{ username : 'user', database : 'foo', password : '<redacted>', type: 'KERBEROS' }"));
    }
}
