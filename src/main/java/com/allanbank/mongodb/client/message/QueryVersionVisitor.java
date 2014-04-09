/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.VisitorAdapter;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.GeospatialOperator;
import com.allanbank.mongodb.builder.MiscellaneousOperator;
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
        else if ("$maxTimeMS".equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    Find.MAX_TIMEOUT_VERSION);
        }
        else if (MiscellaneousOperator.TEXT.getToken().equals(name)) {
            myRequiredServerVersion = Version.later(myRequiredServerVersion,
                    MiscellaneousOperator.TEXT.getVersion());
        }
        else if (GeospatialOperator.UNIQUE_DOCS_MODIFIER.equals(name)) {
            myMaximumServerVersion = Version.earlier(myMaximumServerVersion,
                    GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION);
        }
    }
}
