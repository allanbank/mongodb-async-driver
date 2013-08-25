/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.easymock.EasyMock.createMock;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.junit.Test;

import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * MongoClientConfigurationTest provides tests for the
 * {@link MongoClientConfiguration} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("boxing")
public class MongoClientConfigurationTest {

    /**
     * Test method for
     * {@link MongoClientConfiguration#addCredential(Credential)} .
     */
    @Test
    public void testAddCredentials() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        final Credential.Builder builder = Credential.builder()
                .userName("allanbank")
                .password("super_secret_password".toCharArray());

        assertFalse(config.isAuthenticating());

        config.addCredential(builder);

        assertTrue(config.isAuthenticating());
        assertThat(config.getCredentials(), hasItem(builder.build()));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#addServer(java.net.InetSocketAddress)}.
     */
    @Test
    public void testAddServer() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoClientConfiguration config = new MongoClientConfiguration();

        config.addServer(addr1);
        config.addServer(addr2);

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
    }

    /**
     * Test method for {@link MongoClientConfiguration#addServer(String)}.
     */
    @Test
    public void testAddServerString() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        config.addServer("foo:1234");
        config.addServer("bar:1234");

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#authenticate(String, String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testAuthenticate() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertFalse(config.isAuthenticating());

        config.authenticate("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());

        assertThat(
                config.getCredentials(),
                hasItem(Credential.builder().userName("allanbank")
                        .password("super_secret_password".toCharArray())
                        .database("local").mongodbCR().build()));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#authenticateAsAdmin(String, String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testAuthenticateAsAdmin() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertFalse(config.isAuthenticating());

        config.authenticateAsAdmin("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());

        assertThat(
                config.getCredentials(),
                hasItem(Credential.builder().userName("allanbank")
                        .password("super_secret_password".toCharArray())
                        .database(Credential.ADMIN_DB).mongodbCR().build()));
    }

    /**
     * Test method for {@link MongoDbConfiguration#authenticate(String, String)}
     * .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testAuthenticateSetDBFirst() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertFalse(config.isAuthenticating());
        assertNull(config.getUserName());
        assertNull(config.getPasswordHash());

        config.setDefaultDatabase("foo");
        config.authenticate("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertEquals(1, config.getCredentials().size());
        final Credential cred = config.getCredentials().iterator().next();
        assertEquals("foo", cred.getDatabase());
        assertEquals("allanbank", cred.getUserName());
        assertArrayEquals("super_secret_password".toCharArray(),
                cred.getPassword());
    }

    /**
     * Test method for {@link MongoDbConfiguration#authenticate(String, String)}
     * .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testAuthenticateSetDBSecond() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertFalse(config.isAuthenticating());
        assertNull(config.getUserName());
        assertNull(config.getPasswordHash());

        config.authenticate("allanbank", "super_secret_password");
        config.setDefaultDatabase("foo");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertEquals(1, config.getCredentials().size());
        final Credential cred = config.getCredentials().iterator().next();
        assertEquals("foo", cred.getDatabase());
        assertEquals("allanbank", cred.getUserName());
        assertArrayEquals("super_secret_password".toCharArray(),
                cred.getPassword());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(com.allanbank.mongodb.MongoClientConfiguration)}
     * .
     */
    @Test
    public void testClone() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr1, addr2).clone();

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());

        assertThat(config.getCredentials().size(), is(0));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration()}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoClientConfiguration() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(0, config.getServers().size());
        assertNotNull(config.getThreadFactory());
        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());

        assertThat(config.getCredentials().size(), is(0));

        assertNull(config.getPasswordHash());
        assertNull(config.getUserName());
        assertFalse(config.isAdminUser());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(java.net.InetSocketAddress[])}
     * .
     */
    @Test
    public void testMongoClientConfigurationInetSocketAddressArray() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr1, addr2);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertThat(config.getCredentials().size(), is(0));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(com.allanbank.mongodb.MongoClientConfiguration)}
     * .
     */
    @Test
    public void testMongoClientConfigurationMongoClientConfiguration() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new MongoClientConfiguration(addr1, addr2));

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoFsync() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?fsync;wtimeout=1005");

        assertEquals(Durability.fsyncDurable(1005),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoJournal() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?journal=true;wtimeout=1000");

        assertEquals(Durability.journalDurable(1000),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoReplica() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?w=2;wtimeout=1007");

        assertEquals(Durability.replicaDurable(2, 1007),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoReplicaTimeout() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?wtimeout=1008");

        assertEquals(Durability.replicaDurable(1008),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUri() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoUriAck() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?replicaSet=set1;safe=true");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
        assertEquals("db", config.getDefaultDatabase());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoUriAdminUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://user:pass:ord@foo/");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());

        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());

        assertThat(config.getCredentials(), hasItem(Credential.builder()
                .userName("user").password("pass:ord".toCharArray())
                .mongodbCR().build()));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriAllProps() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?replicaSet=set1;safe=false&w=1&wtimeout=100&fsync=false&fsync&journal=false&"
                        + "connectTIMEOUTMS=1000&socketTimeOUTms=2000;autoDiscoverServers=false;maxConnectionCount=5&"
                        + "maxPendingOperationsPerConnection=101&reconnectTimeoutMS=250&useSoKeepalive=false&foo&safe=false");

        assertEquals(1000, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(5, config.getMaxConnectionCount());
        assertEquals(101, config.getMaxPendingOperationsPerConnection());
        assertEquals(2000, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertFalse(config.isAuthenticating());

        assertThat(config.getCredentials().size(), is(0));
        assertFalse(config.isAutoDiscoverServers());
        assertFalse(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriBadFieldValue() {
        new MongoClientConfiguration("mongodb://foo/db?socketTimeOUTms=alpha");
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoUriExplicitAdminUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://user:pass:ord@foo/admin?");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());

        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());

        assertThat(
                config.getCredentials(),
                hasItem(Credential.builder().userName("user")
                        .password("pass:ord".toCharArray()).database("admin")
                        .authenticationType(Credential.MONGODB_CR).build()));

    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriIllegalValue() {
        new MongoClientConfiguration("mongo://foo?w=foo");
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNoPassword() {
        new MongoClientConfiguration("mongo://user@foo");
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNoServer() {
        new MongoClientConfiguration("mongodb:///");
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNull() {
        new MongoClientConfiguration((String) null);
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriPrimary() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?replicaSet=set1;slaveOk=false");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriSecondary() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo/db?replicaSet=set1;slaveOk=true");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.SECONDARY,
                config.getDefaultReadPreference());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriTwoServers() {
        final String addr1 = "foo:27017";
        final String addr2 = "bar:1234";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://foo,bar:1234");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoUriUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://user:pass:ord@foo/db");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());

        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());

        assertThat(
                config.getCredentials(),
                hasItem(Credential.builder().userName("user")
                        .password("pass:ord".toCharArray()).database("db")
                        .authenticationType(Credential.MONGODB_CR).build()));
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testMongoUriWithIPV6() {
        final String addr1 = "fe80::868f:69ff:feb2:95d4";

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://" + addr1);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize(new InetSocketAddress(addr1, 27017))),
                config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(MongoClientConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriWithIPV6LastTupleLooksLikePort() {
        final String addr1 = "fe80::868f:69ff:feb2";
        final int port = 9534;

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://" + addr1 + ":" + port);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize(new InetSocketAddress(addr1, port))),
                config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @Test
    public void testMongoUriWithIPV6WithPort() {
        final String addr1 = "fe80::868f:69ff:feb2:9534:12345";
        final int port = 12345;

        final MongoClientConfiguration config = new MongoClientConfiguration(
                "mongodb://" + addr1 + ":" + port);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());

        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(ServerNameUtils
                .normalize(new InetSocketAddress(addr1, port))),
                config.getServers());
        assertNotNull(config.getThreadFactory());

        assertFalse(config.isAuthenticating());
        assertThat(config.getCredentials().size(), is(0));
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#MongoClientConfiguration(String)} .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriWrongPrefix() {
        new MongoClientConfiguration("mongo://foo");
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setAutoDiscoverServers(boolean)}.
     */
    @Test
    public void testSetAutoDiscoverServers() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertTrue(config.isAutoDiscoverServers());
        config.setAutoDiscoverServers(false);
        assertFalse(config.isAutoDiscoverServers());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setConnectionModel(ConnectionModel)}.
     */
    @Test
    public void testSetConnectionModel() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        // Check the default.
        assertEquals(ConnectionModel.RECEIVER_THREAD,
                config.getConnectionModel());

        config.setConnectionModel(ConnectionModel.SENDER_RECEIVER_THREAD);
        assertEquals(ConnectionModel.SENDER_RECEIVER_THREAD,
                config.getConnectionModel());
        config.setConnectionModel(ConnectionModel.RECEIVER_THREAD);
        assertEquals(ConnectionModel.RECEIVER_THREAD,
                config.getConnectionModel());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setConnectTimeout(int)}.
     */
    @Test
    public void testSetConnectTimeout() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(0, config.getConnectTimeout());
        config.setConnectTimeout(30000);
        assertEquals(30000, config.getConnectTimeout());
        config.setConnectTimeout(0);
        assertEquals(0, config.getConnectTimeout());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setDefaultDatabase}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSetDefaultDataabse() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(MongoClientConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());
        config.setDefaultDatabase("foo");
        assertEquals("foo", config.getDefaultDatabase());
        config.setDefaultDatabase(MongoClientConfiguration.DEFAULT_DB_NAME);
        assertEquals(MongoClientConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setDefaultDurability(com.allanbank.mongodb.Durability)}
     * .
     */
    @Test
    public void testSetDefaultDurability() {

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(Durability.NONE, config.getDefaultDurability());
        config.setDefaultDurability(Durability.ACK);
        assertEquals(Durability.ACK, config.getDefaultDurability());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setDefaultReadPreference}
     * .
     */
    @Test
    public void testSetDefaultReadPreference() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
        config.setDefaultReadPreference(ReadPreference.SECONDARY);
        assertEquals(ReadPreference.SECONDARY,
                config.getDefaultReadPreference());
        config.setDefaultReadPreference(null);
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setExecutor(Executor)} .
     */
    @Test
    public void testSetExecutor() {
        final Executor executor = Executors.newSingleThreadExecutor();

        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertNull(config.getExecutor());

        config.setExecutor(executor);

        assertSame(executor, config.getExecutor());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setLockType(LockType)}.
     */
    @Test
    public void testSetLockType() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(LockType.MUTEX, config.getLockType());
        config.setLockType(LockType.LOW_LATENCY_SPIN);
        assertEquals(LockType.LOW_LATENCY_SPIN, config.getLockType());
        config.setLockType(LockType.MUTEX);
        assertEquals(LockType.MUTEX, config.getLockType());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setMaxConnectionCount(int)}.
     */
    @Test
    public void testSetMaxConnectionCount() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(3, config.getMaxConnectionCount());
        config.setMaxConnectionCount(1);
        assertEquals(1, config.getMaxConnectionCount());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setMaxPendingOperationsPerConnection(int)}
     * .
     */
    @Test
    public void testSetMaxPendingOperationsPerConnection() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        config.setMaxPendingOperationsPerConnection(256);
        assertEquals(256, config.getMaxPendingOperationsPerConnection());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setMaxSecondaryLag(long)}
     * .
     */
    @Test
    public void testSetMaxSecondaryLag() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(TimeUnit.MINUTES.toMillis(5), config.getMaxSecondaryLag());
        config.setMaxSecondaryLag(TimeUnit.MINUTES.toMillis(25));
        assertEquals(TimeUnit.MINUTES.toMillis(25), config.getMaxSecondaryLag());
        config.setMaxSecondaryLag(TimeUnit.MINUTES.toMillis(5));
        assertEquals(TimeUnit.MINUTES.toMillis(5), config.getMaxSecondaryLag());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setReadTimeout(int)}.
     */
    @Test
    public void testSetReadTimeout() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(0, config.getReadTimeout());
        config.setReadTimeout(30000);
        assertEquals(30000, config.getReadTimeout());
        config.setReadTimeout(0);
        assertEquals(0, config.getReadTimeout());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setReconnectTimeout(int)}
     * .
     */
    @Test
    public void testSetReconnectTimeout() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertEquals(0, config.getReconnectTimeout());
        config.setReconnectTimeout(30000);
        assertEquals(30000, config.getReconnectTimeout());
        config.setReconnectTimeout(0);
        assertEquals(0, config.getReconnectTimeout());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setServers(java.util.List)}.
     */
    @Test
    public void testSetServers() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoClientConfiguration config = new MongoClientConfiguration();

        config.setServers(Arrays.asList(addr1, addr2));

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());

        config.setServers(null);

        assertEquals(0, config.getServers().size());
    }

    /**
     * Test method for {@link MongoClientConfiguration#setDefaultReadPreference}
     * .
     */
    @Test
    public void testSetSocketFactory() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        final SocketFactory custom = createMock(SocketFactory.class);

        assertSame(SocketFactory.getDefault(), config.getSocketFactory());
        config.setSocketFactory(custom);
        assertSame(custom, config.getSocketFactory());
        config.setSocketFactory(null);
        assertSame(SocketFactory.getDefault(), config.getSocketFactory());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setThreadFactory(java.util.concurrent.ThreadFactory)}
     * .
     */
    @Test
    public void testSetThreadFactory() {
        final ThreadFactory tf = Executors.defaultThreadFactory();

        final MongoClientConfiguration config = new MongoClientConfiguration();

        config.setThreadFactory(tf);

        assertSame(tf, config.getThreadFactory());
    }

    /**
     * Test method for
     * {@link MongoClientConfiguration#setUsingSoKeepalive(boolean)} .
     */
    @Test
    public void testSetUsingSoKeepalive() {
        final MongoClientConfiguration config = new MongoClientConfiguration();

        assertTrue(config.isUsingSoKeepalive());
        config.setUsingSoKeepalive(false);
        assertFalse(config.isUsingSoKeepalive());
    }

}
