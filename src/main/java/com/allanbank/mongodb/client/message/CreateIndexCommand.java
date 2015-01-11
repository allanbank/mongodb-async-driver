/*
 * #%L
 * CreateIndexCommand.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.VersionRange;

/**
 * Helper class to make generating a {@code createIndexes} command easier.
 *
 * @see <a
 *      href="http://docs.mongodb.org/manual/reference/command/createIndexes">createIndexes
 *      reference manual.</a>
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CreateIndexCommand
        extends Command {
    /**
     * The first version of MongoDB to support the {@code createIndexes}
     * command.
     */
    public static final Version REQUIRED_VERSION = Version.VERSION_2_6;

    /** The required server version range for the {@code createIndexes} command. */
    public static final VersionRange REQUIRED_VERSION_RANGE = VersionRange
            .minimum(REQUIRED_VERSION);

    /**
     * Generates a name for the index based on the keys.
     *
     * @param keys
     *            The keys for the index.
     * @return The name for the index.
     */
    public static String buildIndexName(final Element... keys) {
        final StringBuilder nameBuilder = new StringBuilder();
        for (final Element key : keys) {
            if (nameBuilder.length() > 0) {
                nameBuilder.append('_');
            }
            nameBuilder.append(key.getName().replace(' ', '_'));
            nameBuilder.append("_");
            if (key instanceof NumericElement) {
                nameBuilder.append(((NumericElement) key).getIntValue());
            }
            else {
                nameBuilder.append(key.getValueAsString());
            }
        }
        return nameBuilder.toString();
    }

    /**
     * Constructs the {@code createIndexes} command.
     *
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not
     *            {@link Command#COMMAND_COLLECTION $cmd}.
     * @param keys
     *            The index keys for the index.
     * @param indexName
     *            The name of the index.
     * @param options
     *            Options for the index. May be <code>null</code>.
     * @return The {@code createIndexes} command.
     */
    private static Document buildCommand(final String collectionName,
            final Element[] keys, final String indexName,
            final DocumentAssignable options) {
        final DocumentBuilder builder = BuilderFactory.start();

        builder.add("createIndexes", collectionName);

        final DocumentBuilder index = builder.pushArray("indexes").push();
        final DocumentBuilder keysDoc = index.push("key");
        for (final Element key : keys) {
            keysDoc.add(key);
        }

        index.add("name", indexName == null ? buildIndexName(keys) : indexName);

        if (options != null) {
            for (final Element option : options.asDocument()) {
                index.add(option);
            }
        }

        return builder.build();
    }

    /**
     * Create a new CreateIndexCommand.
     *
     * @param databaseName
     *            The name of the database.
     * @param collectionName
     *            The name of the collection the command is using. This should
     *            be the real collection and not
     *            {@link Command#COMMAND_COLLECTION $cmd}.
     * @param keys
     *            The index keys for the index.
     * @param indexName
     *            The name of the index.
     * @param options
     *            Options for the index. May be <code>null</code>.
     */
    public CreateIndexCommand(final String databaseName,
            final String collectionName, final Element[] keys,
            final String indexName, final DocumentAssignable options) {
        super(databaseName, collectionName, buildCommand(collectionName, keys,
                indexName, options), ReadPreference.PRIMARY,
                REQUIRED_VERSION_RANGE);
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     *
     * @param object
     *            The object to compare to.
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            result = super.equals(object);
        }
        return result;
    }

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + super.hashCode();
        return result;
    }
}
