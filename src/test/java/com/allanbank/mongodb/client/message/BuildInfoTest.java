/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;

/**
 * BuildInfoTest provides tests for the {@link BuildInfo} command.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BuildInfoTest {

    /**
     * Test method for {@link BuildInfo#BuildInfo()}.
     */
    @Test
    public void testBuildInfo() {
        final BuildInfo buildInfo = new BuildInfo();

        assertEquals(AdminCommand.ADMIN_DATABASE, buildInfo.getDatabaseName());
        assertEquals(Command.COMMAND_COLLECTION, buildInfo.getCollectionName());
        assertEquals(-1, buildInfo.getNumberToReturn());
        assertEquals(0, buildInfo.getNumberToSkip());
        assertEquals(BuildInfo.BUILD_INFO, buildInfo.getQuery());
        assertNull(buildInfo.getReturnFields());
        assertFalse(buildInfo.isAwaitData());
        assertFalse(buildInfo.isExhaust());
        assertFalse(buildInfo.isNoCursorTimeout());
        assertFalse(buildInfo.isPartial());
        assertSame(ReadPreference.PRIMARY, buildInfo.getReadPreference());
        assertFalse(buildInfo.isTailable());
    }

}
