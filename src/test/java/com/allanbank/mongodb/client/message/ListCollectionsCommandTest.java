/*
 * #%L
 * ListCollectionsCommandTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.ListCollections;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.error.ServerVersionException;

/**
 * ListCollectionsCommand provides tests for the {@link ListCollectionsCommand}
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */

public class ListCollectionsCommandTest {

    /**
     * Test method for
     * {@link ListCollectionsCommand#ListCollectionsCommand(String, ListCollections, ReadPreference, boolean)}
     * .
     */
    @Test
    public void testListCollectionsCommand() {
        final Document query = where("name").equals("abc").build();
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.add("listCollections", 1);
        expectedCommand.add("filter", query);
        expectedCommand.add("maxTimeMS", 123L);

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final ListCollectionsCommand cmd = new ListCollectionsCommand("db",
                request, ReadPreference.PREFER_PRIMARY, false);

        assertThat(cmd.getBatchSize(), is(101));
        assertThat(cmd.getCollectionName(), is("system.namespaces"));
        assertThat(cmd.getCommand(), is(expectedCommand.build()));
        assertThat(cmd.getDatabaseName(), is("db"));
        assertThat(cmd.getLimit(), is(202));
        assertThat(cmd.getOperationName(), is("listCollections"));
        assertThat(cmd.getReadPreference(), is(ReadPreference.PREFER_PRIMARY));
        assertThat(cmd.getRequiredVersionRange(), nullValue());
        assertThat(cmd.getRoutingDocument(),
                is((Document) EmptyDocument.INSTANCE));
    }

    /**
     * Test method for
     * {@link ListCollectionsCommand#ListCollectionsCommand(String, ListCollections, ReadPreference, boolean)}
     * .
     */
    @Test
    public void testListCollectionsCommandMinimal() {
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.add("listCollections", 1);

        final ListCollections request = ListCollections.builder().build();

        final ListCollectionsCommand cmd = new ListCollectionsCommand("db",
                request, ReadPreference.PRIMARY, false);

        assertThat(cmd.getBatchSize(), is(0));
        assertThat(cmd.getCollectionName(), is("system.namespaces"));
        assertThat(cmd.getCommand(), is(expectedCommand.build()));
        assertThat(cmd.getDatabaseName(), is("db"));
        assertThat(cmd.getLimit(), is(0));
        assertThat(cmd.getOperationName(), is("listCollections"));
        assertThat(cmd.getReadPreference(), is(ReadPreference.PRIMARY));
        assertThat(cmd.getRequiredVersionRange(), nullValue());
        assertThat(cmd.getRoutingDocument(),
                is((Document) EmptyDocument.INSTANCE));
    }

    /**
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPost277() {
        final Document query = where("name").equals("abc").build();

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
                ReadPreference.PREFER_PRIMARY, false);

        assertThat(cmd.transformFor(ListCollectionsCommand.COMMAND_VERSION),
                sameInstance(cmd));
    }

    /**
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277Minimal() {
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.add("name", ListCollectionsCommand.NON_INDEX_REGEX);

        final ListCollections request = ListCollections.builder().build();

        final Message cmd = new ListCollectionsCommand("db", request,
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
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277NoNameInQuery() {
        final Document query = Find.ALL;
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.push("$query").add("name",
                ListCollectionsCommand.NON_INDEX_REGEX);
        expectedQuery.add("$maxTimeMS", 123L);

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
                ReadPreference.PREFER_PRIMARY, false);

        final Message transformed = cmd.transformFor(Version.VERSION_2_6);

        assertThat(transformed, Matchers.instanceOf(Query.class));

        final Query cmdQuery = (Query) transformed;
        assertThat(cmdQuery.getQuery(), is(expectedQuery.build()));
        assertThat(cmdQuery.getBatchSize(), is(101));
        assertThat(cmdQuery.getLimit(), is(202));
        assertThat(cmdQuery.getReadPreference(),
                is(ReadPreference.PREFER_PRIMARY));
    }

    /**
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277NoNameInQuerySharded() {
        final Document query = Find.ALL;
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.push("$query").add("name",
                ListCollectionsCommand.NON_INDEX_REGEX);
        expectedQuery.add("$maxTimeMS", 123L);
        expectedQuery.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_PRIMARY.asDocument());

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
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

    /**
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277NoNameInQueryShardedPrimary() {
        final Document query = Find.ALL;
        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery.push("$query").add("name",
                ListCollectionsCommand.NON_INDEX_REGEX);
        expectedQuery.add("$maxTimeMS", 123L);

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
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
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277WithNameInQuery() {
        final Document query = where("name").equals("abc").asDocument();
        final Document queryWithDb = where("name").equals("db.abc")
                .asDocument();

        final DocumentBuilder expectedQuery = BuilderFactory.start();
        expectedQuery
                .push("$query")
                .pushArray("$and")
                .add(queryWithDb)
                .add(where("name").matches(
                        ListCollectionsCommand.NON_INDEX_REGEX));
        expectedQuery.add("$maxTimeMS", 123L);

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
                ReadPreference.PREFER_PRIMARY, false);

        final Message transformed = cmd.transformFor(Version.VERSION_2_6);

        assertThat(transformed, Matchers.instanceOf(Query.class));

        final Query cmdQuery = (Query) transformed;
        assertThat(cmdQuery.getQuery(), is(expectedQuery.build()));
        assertThat(cmdQuery.getBatchSize(), is(101));
        assertThat(cmdQuery.getLimit(), is(202));
        assertThat(cmdQuery.getReadPreference(),
                is(ReadPreference.PREFER_PRIMARY));
    }

    /**
     * Test method for {@link ListCollectionsCommand#transformFor(Version)}.
     */
    @Test
    public void testTransformForPre277WithNameInQueryButUsesRegEx() {
        final Document query = where("name").matches(
                ListCollectionsCommand.NON_INDEX_REGEX).asDocument();

        final ListCollections request = ListCollections.builder()
                .batchSize(101).limit(202)
                .maximumTime(123, TimeUnit.MILLISECONDS).query(query)
                .readPreference(ReadPreference.PREFER_SECONDARY).build();

        final Message cmd = new ListCollectionsCommand("db", request,
                ReadPreference.PREFER_PRIMARY, false);
        try {
            cmd.transformFor(Version.VERSION_2_6);
            fail("Should have thrown a ServerVersionException");
        }
        catch (final ServerVersionException expected) {
            assertThat(expected.getActualVersion(), is(Version.VERSION_2_6));
            assertThat(expected.getRequiredVersion(),
                    is(ListCollectionsCommand.COMMAND_VERSION));
            assertThat(expected.getMaximumVersion(), is(Version.UNKNOWN));
            assertThat(expected.getSentMessage(), sameInstance(cmd));
        }
    }
}
