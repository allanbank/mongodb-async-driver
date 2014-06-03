/*
 * #%L
 * BuildInfoTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
        assertThat(buildInfo.getRequiredVersionRange(), nullValue());
        assertThat(buildInfo.getOperationName(), is("buildinfo"));
    }

}
