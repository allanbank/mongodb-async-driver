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

        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
    }

    /**
     * Test method for {@link MongoDbConfiguration#addServer(String)}.
     */
    @Test
    public void testAddServerString() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo", 1234);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

        final MongoDbConfiguration config = new MongoDbConfiguration();

        config.addServer("foo:1234");
        config.addServer("bar:1234");

        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
    }

    /**
     * Test method for {@link MongoDbConfiguration#asHex(byte[])}.
     */
    @Test
    public void testAsHex() {
        assertEquals("00",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x00 }));
        assertEquals("01",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x01 }));
        assertEquals("02",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x02 }));
        assertEquals("03",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x03 }));
        assertEquals("04",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x04 }));
        assertEquals("05",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x05 }));
        assertEquals("06",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x06 }));
        assertEquals("07",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x07 }));
        assertEquals("08",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x08 }));
        assertEquals("09",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x09 }));
        assertEquals("0a",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0a }));
        assertEquals("0b",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0b }));
        assertEquals("0c",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0c }));
        assertEquals("0d",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0d }));
        assertEquals("0e",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0e }));
        assertEquals("0f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x0f }));
        assertEquals("1f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x1f }));
        assertEquals("2f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x2f }));
        assertEquals("3f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x3f }));
        assertEquals("4f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x4f }));
        assertEquals("5f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x5f }));
        assertEquals("6f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x6f }));
        assertEquals("7f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x7f }));
        assertEquals("8f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x8f }));
        assertEquals("9f",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0x9f }));
        assertEquals("af",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xaf }));
        assertEquals("bf",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xbf }));
        assertEquals("cf",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xcf }));
        assertEquals("df",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xdf }));
        assertEquals("ef",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xef }));
        assertEquals("ff",
                MongoDbConfiguration.asHex(new byte[] { (byte) 0xff }));
    }

    /**
     * Test method for {@link MongoDbConfiguration#authenticate(String, String)}
     * .
     */
    @Test
    public void testAuthenticate() {

        final MongoDbConfiguration config = new MongoDbConfiguration();

        assertFalse(config.isAuthenticating());
        assertNull(config.getUsername());
        assertNull(config.getPasswordHash());

        config.authenticate("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUsername());
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
        assertNull(config.getUsername());
        assertNull(config.getPasswordHash());

        config.authenticateAsAdmin("allanbank", "super_secret_password");

        assertTrue(config.isAuthenticating());
        assertEquals("allanbank", config.getUsername());
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
        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUsername());
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
        assertNull(config.getUsername());
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
        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUsername());
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
        assertEquals(Arrays.asList(addr1, addr2), config.getServers());
        assertNotNull(config.getThreadFactory());
        assertNull(config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertNull(config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertNull(config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertNull(config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertNull(config.getUsername());
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
    public void testMongoUri() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertNull(config.getUsername());
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
    public void testMongoUriAdminUserNamePassword() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertEquals("user", config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

        final MongoDbConfiguration config = new MongoDbConfiguration(
                "mongodb://foo/db?replicaSet=set1;safe=false&w=1&wtimeout=100&fsync&journal=false&"
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
        assertNull(config.getUsername());
        assertNull(config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertFalse(config.isAutoDiscoverServers());
        assertFalse(config.isUsingSoKeepalive());
    }

    /**
     * Test method for {@link MongoDbConfiguration#MongoDbConfiguration(String)}
     * .
     */
    @Test
    public void testMongoUriExplicitAdminUserNamePassword() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertEquals("user", config.getUsername());
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
    public void testMongoUriTwoServers() {
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);
        final InetSocketAddress addr2 = new InetSocketAddress("bar", 1234);

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
        assertNull(config.getUsername());
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
        final InetSocketAddress addr1 = new InetSocketAddress("foo",
                MongoDbConfiguration.DEFAULT_PORT);

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
        assertEquals("user", config.getUsername());
        assertEquals("3c80f7cd19bca626d409b336def9ec35",
                config.getPasswordHash());
        assertFalse(config.isAdminUser());
        assertTrue(config.isAutoDiscoverServers());
        assertTrue(config.isUsingSoKeepalive());
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

        assertEquals(Arrays.asList(addr1, addr2), config.getServers());

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
