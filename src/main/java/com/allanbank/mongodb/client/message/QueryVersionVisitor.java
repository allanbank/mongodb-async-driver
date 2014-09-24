/*
 * #%L
 * QueryVersionVisitor.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.VisitorAdapter;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.GeospatialOperator;
import com.allanbank.mongodb.builder.MiscellaneousOperator;
import com.allanbank.mongodb.builder.expression.Expressions;
import com.allanbank.mongodb.client.VersionRange;

/**
 * QueryVersionVisitor provides the ability to inspect a query document for the
 * required server version.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryVersionVisitor extends VisitorAdapter {

    /**
     * Helper to returns the required server version to support the query
     * operators.
     * 
     * @param query
     *            The query to inspect.
     * @return The version of the server that is required to support to support
     *         the query. May be {@code null}.
     */
    public static VersionRange version(final Document query) {
        final QueryVersionVisitor visitor = new QueryVersionVisitor();

        if (query != null) {
            query.accept(visitor);
        }

        return VersionRange.range(visitor.getRequiredServerVersion(),
                visitor.getMaximumServerVersion());
    }

    /**
     * The version of the server that removed the ability to process the visited
     * query.
     */
    private Version myMaximumServerVersion;

    /** The required server version to support the query. */
    private Version myRequiredServerVersion;

    /**
     * Creates a new QueryVersionVisitor.
     */
    public QueryVersionVisitor() {
        myMaximumServerVersion = null;
        myRequiredServerVersion = null;
    }

    /**
     * Returns the version of the server that removed the ability to process the
     * visited query.
     * 
     * @return The version of the server that removed the ability to process the
     *         visited query.
     */
    public Version getMaximumServerVersion() {
        return myMaximumServerVersion;
    }

    /**
     * Returns the required server version to support the visited query.
     * 
     * @return The required server version to support the visited query.
     */
    public Version getRequiredServerVersion() {
        return myRequiredServerVersion;
    }

    /**
     * <p>
     * Overridden to determine the version of the server required based on the
     * query operators.
     * </p>
     * {@inheritDoc}
     */
    @SuppressWarnings("deprecation")
    @Override
    protected void visitName(final String name) {
        if (GeospatialOperator.GEO_WITHIN.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    GeospatialOperator.GEO_WITHIN.getVersion());
        }
        else if (GeospatialOperator.INTERSECT.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    GeospatialOperator.INTERSECT.getVersion());
        }
        else if (GeospatialOperator.POLYGON.equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    GeospatialOperator.POLYGON_VERSION);
        }
        else if ("$maxTimeMS".equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Find.MAX_TIMEOUT_VERSION);
        }
        else if (MiscellaneousOperator.COMMENT.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    MiscellaneousOperator.COMMENT.getVersion());
        }
        else if (MiscellaneousOperator.TEXT.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    MiscellaneousOperator.TEXT.getVersion());
        }
        else if (GeospatialOperator.UNIQUE_DOCS_MODIFIER.equals(name)) {
            myMaximumServerVersion = Version.earlier(myMaximumServerVersion,
                    GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION);
        }
        else if (Expressions.DATE_TO_STRING.equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Version.parse("2.7.4"));
        }
        else if (Expressions.CONCATENATE.equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Version.VERSION_2_4);
        }
    }
}
