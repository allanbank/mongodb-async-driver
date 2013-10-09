/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

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
        assertEquals(BuildInfo.BUILD_INFO, buildInfo.getCommand());
        assertSame(ReadPreference.PRIMARY, buildInfo.getReadPreference());
        assertThat(buildInfo.getRequiredServerVersion(), nullValue());
        assertThat(buildInfo.getOperationName(), is("buildinfo"));
    }

}
