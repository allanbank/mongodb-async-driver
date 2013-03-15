/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * ReadPreference encapsulates a {@link Mode} and a set of tag matching
 * documents. The {@link Mode} specified if primary and/or secondary servers in
 * a replica set can be used and which should be tried first. The tag matching
 * documents control which secondary servers can be used.
 * <p>
 * Each tag matching document specified the minimum set of key/values that the
 * secondary server must be tagged with. As an example a tag matching document
 * of <code>{ a : 1, b : 2 }</code> would match a server with the tags
 * <code>{ a : 1, b : 2, c : 3 }</code> but would not match a server with tags
 * <code>{ a : 1, c : 3, d : 4 }</code>.
 * </p>
 * <p>
 * The tag matching documents and server tags must match exactly so a server
 * with tags <code>{ a: true, b : 2 }</code> would not match the
 * <code>{ a : 1, b : 2 }</code> tag matching document. Neither would a server
 * with the tags <code>{ c: 1, b : 2 }</code>.
 * </p>
 * </p> Each tag matching document specifies a disjoint condition so a server
 * has to match only one for the tag matching document. If the tag matching
 * documents <code>{ a : 1, b : 2 }</code> and <code>{ a : 1, c : 3 }</code> are
 * provided then both the server <code>{ a : 1, b : 2, c : 3 }</code> and
 * <code>{ a : 1, c : 3, d : 4 }</code> would match. </p>
 * <p>
 * If no tag matching documents are specified then all secondary servers may be
 * used.
 * </p>
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReadPreference implements Serializable, DocumentAssignable {

    /**
     * {@link ReadPreference} to read from the closest/{@link Mode#NEAREST}
     * primary of secondary server.
     */
    public static final ReadPreference CLOSEST = new ReadPreference(
            Mode.NEAREST);

    /** The name of the field in the query document for the read preferences. */
    public static final String FIELD_NAME = "$readPreference";

    /**
     * {@link ReadPreference} to prefer reading from the primary but to fallback
     * to a secondary if the primary is not available.
     */
    public static final ReadPreference PREFER_PRIMARY = new ReadPreference(
            Mode.PRIMARY_PREFERRED);

    /**
     * {@link ReadPreference} to prefer reading from a secondary but to
     * 'fallback' to a primary if a secondary is not available.
     */
    public static final ReadPreference PREFER_SECONDARY = new ReadPreference(
            Mode.SECONDARY_PREFERRED);

    /** The default {@link ReadPreference} to read from the primary only. */
    public static final ReadPreference PRIMARY = new ReadPreference(
            Mode.PRIMARY_ONLY);

    /**
     * {@link ReadPreference} to read only from a secondary but using any
     * secondary.
     */
    public static final ReadPreference SECONDARY = new ReadPreference(
            Mode.SECONDARY_ONLY);

    /** The serialization version of the class. */
    private static final long serialVersionUID = -2135959854336511332L;

    /**
     * Creates a {@link ReadPreference} to read from the closest/
     * {@link Mode#NEAREST} primary of secondary server.
     * <p>
     * If tag matching documents are specified then only servers matching the
     * specified tag matching documents would be used.
     * </p>
     * <p>
     * If no tag matching documents are specified then returns {@link #CLOSEST}.
     * </p>
     * 
     * @param tagMatchDocuments
     *            Set of tag matching "documents" controlling which servers are
     *            used.
     * @return The creates {@link ReadPreference}.
     */
    public static ReadPreference closest(
            final DocumentAssignable... tagMatchDocuments) {
        if (tagMatchDocuments.length == 0) {
            return CLOSEST;
        }
        return new ReadPreference(Mode.NEAREST, tagMatchDocuments);
    }

    /**
     * Creates a {@link ReadPreference} to prefer reading from the primary but
     * to fallback to a secondary if the primary is not available.
     * <p>
     * If tag matching documents are specified then only secondary servers
     * matching the specified tag matching documents would be used.
     * </p>
     * <p>
     * If no tag matching documents are specified then returns
     * {@link #PREFER_PRIMARY}.
     * </p>
     * 
     * @param tagMatchDocuments
     *            Set of tag matching "documents" controlling which secondary
     *            servers are used.
     * @return The creates {@link ReadPreference}.
     */
    public static ReadPreference preferPrimary(
            final DocumentAssignable... tagMatchDocuments) {
        if (tagMatchDocuments.length == 0) {
            return PREFER_PRIMARY;
        }
        return new ReadPreference(Mode.PRIMARY_PREFERRED, tagMatchDocuments);
    }

    /**
     * Creates a {@link ReadPreference} to prefer reading from a secondary but
     * to 'fallback' to a primary if a secondary is not available.
     * <p>
     * If tag matching documents are specified then only secondary servers
     * matching the specified tag matching documents would be used.
     * </p>
     * <p>
     * If no tag matching documents are specified then returns
     * {@link #PREFER_SECONDARY}.
     * </p>
     * 
     * @param tagMatchDocuments
     *            Set of tag matching "documents" controlling which secondary
     *            servers are used.
     * @return The creates {@link ReadPreference}.
     */
    public static ReadPreference preferSecondary(
            final DocumentAssignable... tagMatchDocuments) {
        if (tagMatchDocuments.length == 0) {
            return PREFER_SECONDARY;
        }
        return new ReadPreference(Mode.SECONDARY_PREFERRED, tagMatchDocuments);
    }

    /**
     * Returns the default {@link ReadPreference} to read from the primary only:
     * {@link #PRIMARY}.
     * 
     * @return The {@link #PRIMARY} {@link ReadPreference}.
     */
    public static ReadPreference primary() {
        return PRIMARY;
    }

    /**
     * Creates a {@link ReadPreference} to read only from a secondary.
     * <p>
     * If tag matching documents are specified then only secondary servers
     * matching the specified tag matching documents would be used.
     * </p>
     * <p>
     * If no tag matching documents are specified then returns
     * {@link #PREFER_SECONDARY}.
     * </p>
     * 
     * @param tagMatchDocuments
     *            Set of tag matching "documents" controlling which secondary
     *            servers are used.
     * @return The creates {@link ReadPreference}.
     */
    public static ReadPreference secondary(
            final DocumentAssignable... tagMatchDocuments) {
        if (tagMatchDocuments.length == 0) {
            return SECONDARY;
        }
        return new ReadPreference(Mode.SECONDARY_ONLY, tagMatchDocuments);
    }

    /**
     * Creates a {@link ReadPreference} to read only from a specific server.
     * <p>
     * Used by the {@link MongoIterator} to ensure cursor fetch and terminate
     * requests use the originating server.
     * </p>
     * <p>
     * <b>Note:</b> Use this form of {@link ReadPreference} with caution. If the
     * specified server fails all requests will fail.
     * </p>
     * 
     * @param address
     *            The server to read from.
     * @return The creates {@link ReadPreference}.
     */
    public static ReadPreference server(final String address) {
        return new ReadPreference(Mode.SERVER, address);
    }

    /** The document form for the ReadPreference. */
    private final Document myDocumentForm;

    /**
     * The read preference mode controlling if primary or secondary servers can
     * be used and which to prefer.
     */
    private final Mode myMode;

    /**
     * The server to read from. Used by the {@link MongoIterator} to ensure
     * cursor fetch and terminate requests use the originating server.
     */
    private final String myServer;

    /** The list of tag matching documents to control the secondaries used. */
    private final List<Document> myTagMatchingDocuments;

    /**
     * Creates a new ReadPreference.
     * 
     * @param mode
     *            The read preference mode controlling if primary or secondary
     *            servers can be used and which to prefer.
     * @param tagMatchDocuments
     *            Set of tag matching "documents" controlling which secondary
     *            servers are used.
     */
    protected ReadPreference(final Mode mode,
            final DocumentAssignable... tagMatchDocuments) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("mode", mode.getToken());

        myMode = mode;
        myServer = null;
        if (tagMatchDocuments.length == 0) {
            myTagMatchingDocuments = Collections.emptyList();
        }
        else {
            myTagMatchingDocuments = new ArrayList<Document>(
                    tagMatchDocuments.length);

            final ArrayBuilder tagsBuilder = builder.pushArray("tags");
            for (final DocumentAssignable assignable : tagMatchDocuments) {
                final Document tags = assignable.asDocument();

                myTagMatchingDocuments.add(tags);
                tagsBuilder.addDocument(tags);
            }
        }

        myDocumentForm = builder.build();
    }

    /**
     * Creates a new ReadPreference.
     * 
     * @param mode
     *            The read preference mode controlling if primary or secondary
     *            servers can be used and which to prefer.
     * @param address
     *            The server to read from.
     */
    protected ReadPreference(final Mode mode, final String address) {
        myMode = mode;
        myServer = address;
        myTagMatchingDocuments = Collections.emptyList();

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addString("mode", mode.getToken());
        myDocumentForm = builder.build();

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the read preference document.
     * </p>
     */
    @Override
    public Document asDocument() {
        return myDocumentForm;
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
            final ReadPreference other = (ReadPreference) object;

            result = myMode.equals(other.myMode)
                    && myTagMatchingDocuments
                            .equals(other.myTagMatchingDocuments)
                    && nullSafeEquals(myServer, other.myServer);
        }
        return result;
    }

    /**
     * Returns the read preference mode controlling if primary or secondary
     * servers can be used and which to prefer.
     * 
     * @return The read preference mode controlling if primary or secondary
     *         servers can be used and which to prefer.
     */
    public Mode getMode() {
        return myMode;
    }

    /**
     * Returns the server to read from. Used by the {@link MongoIterator} to
     * ensure cursor fetch and terminate requests use the originating server.
     * 
     * @return The server to read from.
     */
    public String getServer() {
        return myServer;
    }

    /**
     * Returns the list of tag matching documents to control the secondaries
     * used.
     * 
     * @return The list of tag matching documents to control the secondaries
     *         used.
     */
    public List<Document> getTagMatchingDocuments() {
        return myTagMatchingDocuments;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + myMode.ordinal();
        result = (31 * result) + myTagMatchingDocuments.hashCode();
        return result;
    }

    /**
     * Returns true if the read preference is compatible with the legacy
     * "slaveOk", e.g., is one of {@link Mode#PRIMARY_ONLY},
     * {@link Mode#SECONDARY_ONLY}, or {@link Mode#SERVER} and has no tag
     * matching documents.
     * 
     * @return True if the mode allows reading from secondaries, false
     *         otherwise.
     */
    public boolean isLegacy() {
        return ((myMode == Mode.PRIMARY_ONLY)
                || (myMode == Mode.SECONDARY_ONLY) || (myMode == Mode.SERVER))
                && myTagMatchingDocuments.isEmpty();
    }

    /**
     * Returns true if the mode allows reading from secondaries, false
     * otherwise.
     * 
     * @return True if the mode allows reading from secondaries, false
     *         otherwise.
     */
    public boolean isSecondaryOk() {
        return myMode.isSecondaryOk();
    }

    /**
     * Returns true if this {@link ReadPreference} matches the <tt>tags</tt>
     * document.
     * 
     * @param tags
     *            The tags to be matched against.
     * @return True if this {@link ReadPreference} matches the tags, false
     *         otherwise.
     */
    public boolean matches(final Document tags) {
        if (myTagMatchingDocuments.isEmpty()) {
            return true;
        }

        boolean matches = false;
        final Iterator<Document> tagMatchingDocIter = myTagMatchingDocuments
                .iterator();
        while (tagMatchingDocIter.hasNext() && !matches) {
            final Document tagMatchingDoc = tagMatchingDocIter.next();

            if (tags == null) {
                if (!tagMatchingDoc.iterator().hasNext()) {
                    // Empty tag document matches all.
                    matches = true;
                }
            }
            else {
                matches = true;
                final Iterator<Element> tagMatchingElemIter = tagMatchingDoc
                        .iterator();
                while (tagMatchingElemIter.hasNext() && matches) {
                    final Element tagMatchingElem = tagMatchingElemIter.next();
                    final Element tag = tags.get(tagMatchingElem.getName());

                    // Note tag may be null...
                    if (!fuzzyEquals(tagMatchingElem, tag)) {
                        matches = false;
                    }
                }
            }
        }

        return matches;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a string representation of the read preference..
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append(myMode.name());

        if (myServer != null) {
            builder.append('[');
            builder.append(myServer);
            builder.append(']');
        }
        else if (!myTagMatchingDocuments.isEmpty()) {
            builder.append('[');
            boolean first = true;
            for (final Document tagDoc : myTagMatchingDocuments) {
                if (!first) {
                    builder.append(", ");
                }
                first = false;

                builder.append('{');
                boolean firstElem = true;
                for (final Element element : tagDoc) {
                    if (!firstElem) {
                        builder.append(", ");
                    }
                    firstElem = false;

                    builder.append(element);
                }
                builder.append('}');
            }
            builder.append(']');
        }

        return builder.toString();
    }

    /**
     * Does a null safe equals comparison.
     * 
     * @param rhs
     *            The right-hand-side of the comparison.
     * @param lhs
     *            The left-hand-side of the comparison.
     * @return True if the rhs equals the lhs. Note: nullSafeEquals(null, null)
     *         returns true.
     */
    protected boolean nullSafeEquals(final Object rhs, final Object lhs) {
        return (rhs == lhs) || ((rhs != null) && rhs.equals(lhs));
    }

    /**
     * Compares if the two elements are equals allowing numeric values type to
     * not be a strict match but when casted as a long those values must still
     * compare equal.
     * 
     * @param lhs
     *            The first element to compare. May not be <code>null</code>.
     * @param rhs
     *            The second element to compare. May be <code>null</code>.
     * @return True if the two elements compare equal ignore two
     *         {@link NumericElement}s' specific type.
     */
    private boolean fuzzyEquals(final Element lhs, final Element rhs) {
        // Be fuzzy on the integer/long/double.
        if ((rhs instanceof NumericElement) && (lhs instanceof NumericElement)) {
            final long tagValue = ((NumericElement) rhs).getLongValue();
            final long tagMatchingValue = ((NumericElement) lhs).getLongValue();
            return (tagValue == tagMatchingValue);
        }

        // Otherwise exact match.
        return lhs.equals(rhs);
    }

    /**
     * Hook into serialization to replace <tt>this</tt> object with the local
     * {@link #CLOSEST}, {@link #PREFER_PRIMARY}, {@link #PREFER_SECONDARY},
     * {@link #PRIMARY}, or {@link #SECONDARY} instance as appropriate.
     * 
     * @return Either the {@link #CLOSEST}, {@link #PREFER_PRIMARY},
     *         {@link #PREFER_SECONDARY}, {@link #PRIMARY}, or
     *         {@link #SECONDARY} instance if <tt>this</tt> instance equals one
     *         of those instances otherwise <tt>this</tt> instance.
     */
    private Object readResolve() {
        if (this.equals(CLOSEST)) {
            return CLOSEST;
        }
        else if (this.equals(PREFER_PRIMARY)) {
            return PREFER_PRIMARY;
        }
        else if (this.equals(PREFER_SECONDARY)) {
            return PREFER_SECONDARY;
        }
        else if (this.equals(PRIMARY)) {
            return PRIMARY;
        }
        else if (this.equals(SECONDARY)) {
            return SECONDARY;
        }
        else {
            return this;
        }
    }

    /**
     * Enumeration of the basic {@link ReadPreference} modes of service.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static enum Mode {
        /**
         * Use the nearest (by latency measurement) member of the replica set:
         * either primary or secondary servers are allowed.
         * <p>
         * If tag matching documents are specified then only server matching the
         * specified tag matching documents would be used.
         * </p>
         */
        NEAREST("nearest"),

        /**
         * Reads should only be attempted from the primary member of the replica
         * set.
         */
        PRIMARY_ONLY("primary"),

        /**
         * Read from the primary but in the case of a fault may fallback to a
         * secondary.
         * <p>
         * If tag matching documents are specified and a fallback to a secondary
         * is required then only secondaries matching the specified tag matching
         * documents would be used.
         * </p>
         */
        PRIMARY_PREFERRED("primaryPreferred"),

        /**
         * Do not attempt to read from the primary.
         * <p>
         * If tag matching documents are specified then only secondaries
         * matching the specified tag matching documents would be used.
         * </p>
         */
        SECONDARY_ONLY("secondary"),

        /**
         * Try to first read from a secondary. If none are available "fallback"
         * to the primary.
         * <p>
         * If tag matching documents are specified then only secondaries
         * matching the specified tag matching documents would be used.
         * </p>
         */
        SECONDARY_PREFERRED("secondaryPreferred"),

        /**
         * Do not attempt to read from any server other than the one specified.
         * Used by the {@link MongoIterator} to ensure cursor fetch and
         * terminate requests use the originating server.
         */
        SERVER("server");

        /** The token passed to the mongos server when in a shared environment. */
        private final String myToken;

        /**
         * Creates a new Mode.
         * 
         * @param token
         *            The token passed to the mongos server when in a shared
         *            environment.
         */
        private Mode(final String token) {
            myToken = token;
        }

        /**
         * Returns the token passed to the mongos server when in a shared
         * environment.
         * 
         * @return The token passed to the mongos server when in a shared
         *         environment.
         */
        public String getToken() {
            return myToken;
        }

        /**
         * Returns true if the mode allows reading from secondaries, false
         * otherwise.
         * 
         * @return True if the mode allows reading from secondaries, false
         *         otherwise.
         */
        public boolean isSecondaryOk() {
            return (this != PRIMARY_ONLY);
        }
    }
}
