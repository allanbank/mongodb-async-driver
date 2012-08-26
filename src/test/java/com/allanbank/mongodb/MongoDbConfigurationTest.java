/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

/**
 * MongoDbConfigurationTest provides tests for the {@link MongoDbConfiguration}
 * class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbConfigurationTest {

    /**
     * Test method for
     * {@link MongoDbConfiguration#addServer(java.net.InetSocketAddress)}.
     */
    @Test
    public void testAddServer() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration();

        config.addServer(addr1);
        config.addServer(addr2);

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
    }

    /**
     * Test method for {@link MongoDbConfiguration#addServer(String)}.
     */
    @Test
    public void testAddServerString() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        config.addServer("foo:1234");
        config.addServer("bar:1234");

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
    }

    /**
     * Test method for {@link MongoDbConfiguration#authenticate(String, String)}
     * .
     */
    @Test
    public void testAuthenticate() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertFalse(config.isAuthenticating());
        assertNull(config.getUserName());
        assertNull(config.getPasswordHash());

        config.authenticate("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#authenticateAsAdmin(String, String)} .
     */
    @Test
    public void testAuthenticateAsAdmin() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertFalse(config.isAuthenticating());
        assertNull(config.getUserName());
        assertNull(config.getPasswordHash());

        config.authenticateAsAdmin("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUserName());
        assertEquals("fc3b4ed71943faaefb6c21fdffee3e95",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#MongoDbConfiguration(com.allanbank.mongodb.MongoDbConfiguration)}
     * .
     */
    @Test
    public void testClone() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration(addr1,
                addr2).clone();

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration()}.
     */
    @Test
    public void testMongoDbConfiguration() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(0, config.getServers().size());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#MongoDbConfiguration(java.net.InetSocketAddress[])}
     * .
     */
    @Test
    public void testMongoDbConfigurationInetSocketAddressArray() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration(addr1,
                addr2);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#MongoDbConfiguration(com.allanbank.mongodb.MongoDbConfiguration)}
     * .
     */
    @Test
    public void testMongoDbConfigurationMongoDbConfiguration() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration(
                new MongoDbConfiguration(addr1, addr2));

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoFsync() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?fsync;wtimeout=1005");

        assertEquals(Durability.fsyncDurable(1005),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoJournal() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?journal=true;wtimeout=1000");

        assertEquals(Durability.journalDurable(1000),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoReplica() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?w=2;wtimeout=1007");

        assertEquals(Durability.replicaDurable(2, 1007),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoReplicaTimeout() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?wtimeout=1008");

        assertEquals(Durability.replicaDurable(1008),
                config.getDefaultDurability());

        assertEquals(0, config.getConnectTimeout());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriWithIPV6() {
        final String addr1 = "fe80::868f:69ff:feb2:95d4";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://" + addr1);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1 + ":27017"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());

    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriWithIPV6LastTupleLooksLikePort() {
        final String addr1 = "fe80::868f:69ff:feb2:9534";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://" + addr1);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1 + ":27017"), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());

    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriWithIPV6WithPort() {
        final String addr1 = "fe80::868f:69ff:feb2:9534:12345";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://" + addr1);

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());

    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUri() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());

    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriAck() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?replicaSet=set1;safe=true");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.ACK, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
        assertEquals("db", config.getDefaultDatabase());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriAdminUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://user:pass:ord@foo/");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriAllProps() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?replicaSet=set1;safe=false&w=1&wtimeout=100&fsync=false&fsync&journal=false&"
                        + "connectTIMEOUTMS=1000&socketTimeOUTms=2000;autoDiscoverServers=false;maxConnectionCount=5&"
                        + "maxPendingOperationsPerConnection=101&reconnectTimeoutMS=250&useSoKeepalive=false&foo&safe=false");

        assertEquals(1000, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(5, config.getMaxConnectionCount());
        assertEquals(101, config.getMaxPendingOperationsPerConnection());
        assertEquals(2000, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertFalse(config.isAuthenticating());
        assertNull(config.getUserName());
        assertNull(config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertFalse(config.isAutoDiscoverServers());
        assertFalse(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriBadFieldValue() {
        new MongoDbConfiguration("mongodb://foo/db?socketTimeOUTms=alpha");
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriExplicitAdminUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://user:pass:ord@foo/admin?");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertTrue(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriIllegalValue() {
        new MongoDbConfiguration("mongo://foo?w=foo");
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNoPassword() {
        new MongoDbConfiguration("mongo://user@foo");
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNoServer() {
        new MongoDbConfiguration("mongodb:///");
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriNull() {
        new MongoDbConfiguration((String) null);
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriPrimary() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?replicaSet=set1;slaveOk=false");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriSecondary() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?replicaSet=set1;slaveOk=true");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
        assertEquals(ReadPreference.SECONDARY,
                config.getDefaultReadPreference());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriTwoServers() {
        final String addr1 = "foo:27017";
        final String addr2 = "bar:1234";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo,bar:1234");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertNull(config.getPasswordHash());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUserName());
        assertFalse(config.isAuthenticating());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriUserNamePassword() {
        final String addr1 = "foo:27017";

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://user:pass:ord@foo/db");

        assertEquals(0, config.getConnectTimeout());
        assertEquals(Durability.NONE, config.getDefaultDurability());
        assertEquals(3, config.getMaxConnectionCount());
        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        assertEquals(0, config.getReadTimeout());
        assertEquals(Collections.singletonList(addr1), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertTrue(config.isAuthenticating());
        assertEquals("user", config.getUserName());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testMongoUriWrongPrefix() {
        new MongoDbConfiguration("mongo://foo");
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#setAutoDiscoverServers(boolean)}.
     */
    @Test
    public void testSetAutoDiscoverServers() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertTrue(config.isAutoDiscoverServers());
        config.setAutoDiscoverServers(false);
        assertFalse(config.isAutoDiscoverServers());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setConnectTimeout(int)}.
     */
    @Test
    public void testSetConnectTimeout() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(0, config.getConnectTimeout());
        config.setConnectTimeout(30000);
        assertEquals(30000, config.getConnectTimeout());
        config.setConnectTimeout(0);
        assertEquals(0, config.getConnectTimeout());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setDefaultDatabase}.
     */
    @Test
    public void testSetDefaultDataabse() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());
        config.setDefaultDatabase("foo");
        assertEquals("foo", config.getDefaultDatabase());
        config.setDefaultDatabase(MongoDbConfiguration.DEFAULT_DB_NAME);
        assertEquals(MongoDbConfiguration.DEFAULT_DB_NAME,
                config.getDefaultDatabase());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#setDefaultDurability(com.allanbank.mongodb.Durability)}
     * .
     */
    @Test
    public void testSetDefaultDurability() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(Durability.NONE, config.getDefaultDurability());
        config.setDefaultDurability(Durability.ACK);
        assertEquals(Durability.ACK, config.getDefaultDurability());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setDefaultReadPreference}.
     */
    @Test
    public void testSetDefaultReadPreference() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
        config.setDefaultReadPreference(ReadPreference.SECONDARY);
        assertEquals(ReadPreference.SECONDARY,
                config.getDefaultReadPreference());
        config.setDefaultReadPreference(null);
        assertEquals(ReadPreference.PRIMARY, config.getDefaultReadPreference());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setMaxConnectionCount(int)}.
     */
    @Test
    public void testSetMaxConnectionCount() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(3, config.getMaxConnectionCount());
        config.setMaxConnectionCount(1);
        assertEquals(1, config.getMaxConnectionCount());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#setMaxPendingOperationsPerConnection(int)}.
     */
    @Test
    public void testSetMaxPendingOperationsPerConnection() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(1024, config.getMaxPendingOperationsPerConnection());
        config.setMaxPendingOperationsPerConnection(256);
        assertEquals(256, config.getMaxPendingOperationsPerConnection());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setReadTimeout(int)}.
     */
    @Test
    public void testSetReadTimeout() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(0, config.getReadTimeout());
        config.setReadTimeout(30000);
        assertEquals(30000, config.getReadTimeout());
        config.setReadTimeout(0);
        assertEquals(0, config.getReadTimeout());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setReconnectTimeout(int)}.
     */
    @Test
    public void testSetReconnectTimeout() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertEquals(0, config.getReconnectTimeout());
        config.setReconnectTimeout(30000);
        assertEquals(30000, config.getReconnectTimeout());
        config.setReconnectTimeout(0);
        assertEquals(0, config.getReconnectTimeout());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setServers(java.util.List)}.
     */
    @Test
    public void testSetServers() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration();

        config.setServers(Arrays.asList(addr1, addr2));

        assertEquals(Arrays.asList("foo:1234", "bar:1234"), config.getServers());

        config.setServers(null);

        assertEquals(0, config.getServers().size());
    }

    /**
     * Test method for
     * {@link MongoDbConfiguration#setThreadFactory(java.util.concurrent.ThreadFactory)}
     * .
     */
    @Test
    public void testSetThreadFactory() {
        final ThreadFactory tf = Executors.defaultThreadFactory();

        final MongoDbConfiguration config = new MongoDbConfiguration();

        config.setThreadFactory(tf);

        assertSame(tf, config.getThreadFactory());
    }

    /**
     * Test method for {@link MongoDbConfiguration#setUsingSoKeepalive(boolean)}
     * .
     */
    @Test
    public void testSetUsingSoKeepalive() {
        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertTrue(config.isUsingSoKeepalive());
        config.setUsingSoKeepalive(false);
        assertFalse(config.isUsingSoKeepalive());
    }

}
