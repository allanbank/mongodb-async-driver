/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;

/**
 * IsMasterTest provides tests for the {@link IsMaster} message.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IsMasterTest {

    /**
     * Test method for {@link IsMaster#IsMaster()}.
     */
    @Test
    public void testIsMaster() {
        final IsMaster isMaster = new IsMaster();

        assertEquals(AdminCommand.ADMIN_DATABASE, isMaster.getDatabaseName());
        assertEquals(Command.COMMAND_COLLECTION, isMaster.getCollectionName());
        assertEquals(IsMaster.IS_MASTER, isMaster.getCommand());
        assertSame(ReadPreference.PRIMARY, isMaster.getReadPreference());
        assertThat(isMaster.getRequiredVersionRange(), nullValue());
    }
}
