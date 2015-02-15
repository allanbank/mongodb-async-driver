/*
 * #%L
 * ListIndexesCommandTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.builder.ListIndexes;
import com.allanbank.mongodb.client.Message;

/**
 * ListIndexesCommand provides tests for the {@link ListIndexesCommand} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */

public class ListIndexesCommandTest {

    /**
     * Test method for
     * {@link ListIndexesCommand#ListIndexesCommand(String, String, ListIndexes, ReadPreference, boolean)}
     * .
     */
    @Test
    public void testListIndexesCommand() {
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.add("listIndexes", "collection");
        expectedCommand.add("maxTimeMS", 123L);

        final ListIndexes request = ListIndexes.builder().batchSize(101)
                .limit(202).maximumTime(123, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final ListIndexesCommand cmd = new ListIndexesCommand("db",
                "collection", request, ReadPreference.PREFER_PRIMARY, false);

        assertThat(cmd.getBatchSize(), is(101));
        assertThat(cmd.getCollectionName(), is("collection"));
        assertThat(cmd.getCommand(), is(expectedCommand.build()));
        assertThat(cmd.getDatabaseName(), is("db"));
        assertThat(cmd.getLimit(), is(202));
        assertThat(cmd.getOperationName(), is("listIndexes"));
        assertThat(cmd.getReadPreference(), is(ReadPreference.PREFER_PRIMARY));
        assertThat(cmd.getRequiredVersionRange(), nullValue());
        assertThat(cmd.getRoutingDocument(),
                is((Document) EmptyDocument.INSTANCE));
    }

    /**
     * Test method for
     * {@link ListIndexesCommand#ListIndexesCommand(String, String, ListIndexes, ReadPreference, boolean)}
     * .
     */
    @Test
    public void testListIndexesCommandMinimal() {
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.add("listIndexes", "collection");

        final ListIndexes request = ListIndexes.builder().build();

        final ListIndexesCommand cmd = new ListIndexesCommand("db",
                "collection", request, ReadPreference.PRIMARY, false);

        assertThat(cmd.getBatchSize(), is(0));
        assertThat(cmd.getCollectionName(), is("collection"));
        assertThat(cmd.getCommand(), is(expectedCommand.build()));
        assertThat(cmd.getDatabaseName(), is("db"));
        assertThat(cmd.getLimit(), is(0));
        assertThat(cmd.getOperationName(), is("listIndexes"));
        assertThat(cmd.getReadPreference(), is(ReadPreference.PRIMARY));
        assertThat(cmd.getRequiredVersionRange(), nullValue());
        assertThat(cmd.getRoutingDocument(),
                is((Document) EmptyDocument.INSTANCE));
    }

    /**
     * Test method for {@link ListIndexesCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPost277() {
        final ListIndexes request = ListIndexes.builder().batchSize(101)
                .limit(202).maximumTime(123, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListIndexesCommand("db", "collection", request,
                ReadPreference.PREFER_PRIMARY, false);

        assertThat(cmd.transformFor(ListIndexesCommand.COMMAND_VERSION),
                sameInstance(cmd));
    }

    /**
     * Test method for {@link ListIndexesCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277Minimal() {
        final DocumentBuilder expectedQuery = BuilderFactory.start().add("ns",
                "db.collection");

        final ListIndexes request = ListIndexes.builder().build();

        final Message cmd = new ListIndexesCommand("db", "collection", request,
                ReadPreference.PRIMARY, true);

        final Message transformed = cmd.transformFor(Version.VERSION_2_6);

        assertThat(transformed, Matchers.instanceOf(Query.class));

        final Query cmdQuery = (Query) transformed;
        assertThat(cmdQuery.getQuery(), is(expectedQuery.build()));
        assertThat(cmdQuery.getBatchSize(), is(0));
        assertThat(cmdQuery.getLimit(), is(0));
        assertThat(cmdQuery.getReadPreference(), is(ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link ListIndexesCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277NoNameInQueryShardedPrimary() {
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.add("ns", "db.collection").add("$maxTimeMS", 123L);

        final ListIndexes request = ListIndexes.builder().batchSize(101)
                .limit(202).maximumTime(123, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListIndexesCommand("db", "collection", request,
                ReadPreference.PRIMARY, true);

        final Message transformed = cmd.transformFor(Version.VERSION_2_6);

        assertThat(transformed, Matchers.instanceOf(Query.class));

        final Query cmdQuery = (Query) transformed;
        assertThat(cmdQuery.getQuery(), is(expectedQuery.build()));
        assertThat(cmdQuery.getBatchSize(), is(101));
        assertThat(cmdQuery.getLimit(), is(202));
        assertThat(cmdQuery.getReadPreference(), is(ReadPreference.PRIMARY));
    }

    /**
     * Test method for {@link ListIndexesCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277Sharded() {
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.push("$query").add("ns", "db.collection")
                .add("$maxTimeMS", 123L);
        expectedQuery.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_PRIMARY.asDocument());

        final ListIndexes request = ListIndexes.builder().batchSize(101)
                .limit(202).maximumTime(123, TimeUnit.MILLISECONDS)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListIndexesCommand("db", "collection", request,
                ReadPreference.PREFER_PRIMARY, true);

        final Message transformed = cmd.transformFor(Version.VERSION_2_6);

        assertThat(transformed, Matchers.instanceOf(Query.class));

        final Query cmdQuery = (Query) transformed;
        assertThat(cmdQuery.getQuery(), is(expectedQuery.build()));
        assertThat(cmdQuery.getBatchSize(), is(101));
        assertThat(cmdQuery.getLimit(), is(202));
        assertThat(cmdQuery.getReadPreference(),
                is(ReadPreference.PREFER_PRIMARY));
    }
}
