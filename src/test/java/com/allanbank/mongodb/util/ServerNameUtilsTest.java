/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import org.junit.Test;

/**
 * ServerNameUtilsTest provides tests of the {@link ServerNameUtils} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerNameUtilsTest {

    /**
     * Test method for {@link ServerNameUtils#normalize(String)}.
     */
    @Test
    public void testNormalizeString() {
        assertEquals("foo:27017", ServerNameUtils.normalize("foo"));
        assertEquals("foo:27018", ServerNameUtils.normalize("foo:27018"));

        assertEquals("foo:abot:27017", ServerNameUtils.normalize("foo:abot"));

        assertEquals("192.168.0.1:27017",
                ServerNameUtils.normalize("192.168.0.1"));
        assertEquals("192.168.0.1:27018",
                ServerNameUtils.normalize("192.168.0.1:27018"));

        assertEquals("fe80::868f:69ff:feb2:95d4:27017",
                ServerNameUtils.normalize("fe80::868f:69ff:feb2:95d4"));
        assertEquals("fe80::868f:69ff:feb2:95d4:27017",
                ServerNameUtils.normalize("fe80::868f:69ff:feb2:95d4:27017"));
    }

    /**
     * Test method for {@link ServerNameUtils#parse(String)}.
     */
    @Test
    public void testParse() {
        assertEquals(new InetSocketAddress("foo", 27017),
                ServerNameUtils.parse("foo"));
        assertEquals(new InetSocketAddress("foo", 27018),
                ServerNameUtils.parse("foo:27018"));
        assertEquals(new InetSocketAddress("fe80::868f:69ff:feb2:95d4", 27018),
                ServerNameUtils.parse("fe80::868f:69ff:feb2:95d4:27018"));
    }
}
