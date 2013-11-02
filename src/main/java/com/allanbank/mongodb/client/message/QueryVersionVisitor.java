/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.VisitorAdapter;
import com.allanbank.mongodb.builder.GeospatialOperator;

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
    public static Version version(final Document query) {
        final QueryVersionVisitor visitor = new QueryVersionVisitor();

        if (query != null) {
            query.accept(visitor);
        }

        return visitor.getRequiredServerVersion();
    }

    /** The required server version to support the query. */
    private Version myRequiredServerVersion;

    /**
     * Creates a new QueryVersionVisitor.
     */
    public QueryVersionVisitor() {
        myRequiredServerVersion = null;
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
    @Override
    protected void visitName(final String name) {
        if (GeospatialOperator.GEO_WITHIN.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Version.VERSION_2_4);
        }
        else if (GeospatialOperator.INTERSECT.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Version.VERSION_2_4);
        }
        else if ("$maxTimeMS".equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Version.VERSION_2_6);
        }
    }
}
