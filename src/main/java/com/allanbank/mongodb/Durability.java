/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Serializable;
import java.io.StringWriter;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.JsonSerializationVisitor;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.error.JsonParseException;

/**
 * Represents the required durability of writes (inserts, updates, and deletes)
 * on the server.
 * <ul>
 * <li>The lowest durability ({@link #NONE}) has no guarantees that the data
 * will survive a catastrophic server failure.
 * <li>The next level of durability ({@link #ACK}) ensures that the data has
 * been received by the server.
 * <li>The next level of durability ({@link #journalDurable(int)}) ensures that
 * the data is written to the server's journal before returning.
 * <li>Next level ({@link #fsyncDurable(int)}) is to ensure that the data has
 * been fsync()'d to the server's disk.
 * <li>For the highest level of durability ({@link #replicaDurable(int)}), the
 * server ensure that the data has been received by 1 to
 * {@link #replicaDurable(int, int) N} replicas in the replica set. Fine grain
 * control can be achieved by specifying a {@link #replicaDurable(String, int)
 * replication mode} instead of a count.</li>
 * </ul>
 * <p>
 * Generally, increasing the level of durability decreases performance.
 * </p>
 * 
 * @see <a href="http://www.mongodb.org/display/DOCS/Data+Center+Awareness">Data
 *      Center Awareness</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Durability implements Serializable {

    /** The durability that says no durability is required. */
    public final static Durability ACK = new Durability(true, false, false, 1,
            null, 0);

    /**
     * Built in replication mode indicating that more than 50% of the MongoDB
     * replica set servers have received a write.
     */
    public final static String MAJORITY_MODE = "majority";

    /** The durability that says no durability is required. */
    public final static Durability NONE = new Durability(false, false, false,
            0, null, 0);

    /** Serialization version for the class. */
    private static final long serialVersionUID = -6474266523435876385L;

    /**
     * Creates an fsync() durability.
     * <p>
     * Will cause the server to wait for the write to be sync'd to disk. If the
     * server is running with journaling enabled then only the journal will have
     * been sync'd to disk. If running without journaling enabled then will wait
     * for all data files to be sync'd to disk.
     * 
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure that the data has been fsync()'d to
     *         the server's disk.
     */
    public static Durability fsyncDurable(final int waitTimeoutMillis) {
        return new Durability(true, false, 0, waitTimeoutMillis);
    }

    /**
     * Creates an journal durability.
     * <p>
     * Will cause the server to wait for the write to be sync'd to disk as part
     * of the journal. As of MongoDB 2.6 this mode will cause a TBD exception to
     * be thrown if the server is configured without journaling enabled. Prior
     * to MongoDB 2.6 this mode would silently degrade to {@link #ACK}.
     * </p>
     * 
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to the server's
     *         journal before returning.
     */
    public static Durability journalDurable(final int waitTimeoutMillis) {
        return new Durability(false, true, 0, waitTimeoutMillis);
    }

    /**
     * Creates a multiple replica durability.
     * 
     * @param ensureJournaled
     *            If true then ensure that the write has been committed to the
     *            journal in addition to replicated.
     * @param minimumReplicas
     *            The minimum number of replicas to wait for.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to at least
     *         <tt>minimumReplicas</tt> of server's replicas before returning.
     */
    public static Durability replicaDurable(final boolean ensureJournaled,
            final int minimumReplicas, final int waitTimeoutMillis) {
        return new Durability(false, ensureJournaled, minimumReplicas,
                waitTimeoutMillis);
    }

    /**
     * Creates a multiple replica durability.
     * 
     * @param ensureJournaled
     *            If true then ensure that the write has been committed to the
     *            journal in addition to replicated.
     * @param waitForReplicasByMode
     *            If the value is non-<code>null</code> then wait for the
     *            specified replication mode configured on the server. A
     *            built-in mode of {@link #MAJORITY_MODE} is also supported.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to at least
     *         <tt>minimumReplicas</tt> of server's replicas before returning.
     */
    public static Durability replicaDurable(final boolean ensureJournaled,
            final String waitForReplicasByMode, final int waitTimeoutMillis) {
        return new Durability(false, ensureJournaled, waitForReplicasByMode,
                waitTimeoutMillis);
    }

    /**
     * Creates a single replica durability.
     * 
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to at least one
     *         of server's replicas before returning.
     */
    public static Durability replicaDurable(final int waitTimeoutMillis) {
        return new Durability(false, false, 1, waitTimeoutMillis);
    }

    /**
     * Creates a multiple replica durability.
     * 
     * @param minimumReplicas
     *            The minimum number of replicas to wait for.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to at least
     *         <tt>minimumReplicas</tt> of server's replicas before returning.
     */
    public static Durability replicaDurable(final int minimumReplicas,
            final int waitTimeoutMillis) {
        return new Durability(false, false, minimumReplicas, waitTimeoutMillis);
    }

    /**
     * Creates a multiple replica durability.
     * 
     * @param waitForReplicasByMode
     *            If the value is non-<code>null</code> then wait for the
     *            specified replication mode configured on the server. A
     *            built-in mode of {@link #MAJORITY_MODE} is also supported.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     * @return A durability that will ensure the data is written to at least
     *         <tt>minimumReplicas</tt> of server's replicas before returning.
     */
    public static Durability replicaDurable(final String waitForReplicasByMode,
            final int waitTimeoutMillis) {
        return new Durability(false, false, waitForReplicasByMode,
                waitTimeoutMillis);
    }

    /**
     * Converts a string into a Durability, if possible.
     * <p>
     * Two forms of strings can be converted:
     * <ul>
     * <li>A name of the constant (ignoring case):
     * <ul>
     * <li>ACK</li>
     * <li>NONE</li>
     * <li>SAFE - for compatibility with the 10gen MongoDB driver, returns
     * {@link #ACK}.</li>
     * </ul>
     * </li>
     * <li>A JSON document representation of the Durability. The following
     * fields are allowed in the document and the values for each should match
     * those for a {@code getlasterror} command:
     * <ul>
     * <li>w</li>
     * <li>wtimeout</li>
     * <li>fsync</li>
     * <li>j</li>
     * <li>getlasterror</li>
     * </ul>
     * If present the {@code getlasterror} field is ignored. An example JSON
     * document might look like: <blockquote>
     * 
     * <pre>
     * <code>
     * {
     *    w : majority,
     *    wtimeout : 10000,
     * }
     * </code>
     * </pre>
     * 
     * <blockquote></li>
     * </ul>
     * </p>
     * <p>
     * If the string is not parsable in either of these forms then null is
     * returned.
     * 
     * @param value
     *            The string representation of the Durability.
     * @return The Durability represented by the string.
     */
    public static Durability valueOf(final String value) {

        Durability result = null;

        if ("ACK".equalsIgnoreCase(value) || "SAFE".equalsIgnoreCase(value)) {
            result = ACK;
        }
        else if ("NONE".equalsIgnoreCase(value)) {
            result = NONE;
        }
        else {
            // Try and parse as JSON.
            try {
                boolean waitForReply = false;
                boolean waitForFsync = false;
                boolean waitForJournal = false;
                int waitForReplicas = 0;
                String waitForReplicasByMode = null;
                int waitTimeoutMillis = 0;

                final Document d = Json.parse(value);
                for (final Element e : d) {
                    // Skip the getlasterror element.
                    if (!"getlasterror".equalsIgnoreCase(e.getName())) {
                        if ("w".equalsIgnoreCase(e.getName())) {
                            waitForReply = true;
                            if (e instanceof NumericElement) {
                                waitForReplicas = ((NumericElement) e)
                                        .getIntValue();
                            }
                            else if (e instanceof StringElement) {
                                waitForReplicasByMode = ((StringElement) e)
                                        .getValue();
                            }
                            else if (e instanceof SymbolElement) {
                                waitForReplicasByMode = ((SymbolElement) e)
                                        .getSymbol();
                            }
                            else {
                                // Unknown 'w' value type.
                                return null;
                            }
                        }
                        else if ("wtimeout".equalsIgnoreCase(e.getName())) {
                            if (e instanceof NumericElement) {
                                waitTimeoutMillis = ((NumericElement) e)
                                        .getIntValue();
                            }
                            else {
                                // Unknown 'wtimeout' value type.
                                return null;
                            }
                        }
                        else if ("fsync".equalsIgnoreCase(e.getName())) {
                            waitForReply = true;
                            waitForFsync = true;
                        }
                        else if ("j".equalsIgnoreCase(e.getName())) {
                            waitForReply = true;
                            waitForJournal = true;
                        }
                        else {
                            // Unknown field.
                            return null;
                        }
                    }
                }

                result = new Durability(waitForReply, waitForFsync,
                        waitForJournal, waitForReplicas, waitForReplicasByMode,
                        waitTimeoutMillis);
            }
            catch (final JsonParseException error) {
                // Ignore and return null.
                error.getCause(); // Shhh PMD.
            }
        }

        return result;
    }

    /** The durability in document form. */
    private Document myAsDocument;

    /**
     * True if the durability requires that the response wait for an fsync() of
     * the data to complete, false otherwise.
     */
    private final boolean myWaitForFsync;

    /**
     * True if if the durability requires that the response wait for the data to
     * be written to the server's journal, false otherwise.
     */
    private final boolean myWaitForJournal;

    /**
     * If the value is value greater than zero the durability requires that the
     * response wait for the data to be received by a replica and the number of
     * replicas of the data to wait for.
     */
    private final int myWaitForReplicas;

    /**
     * If the value is non-<code>null</code> then wait for the specified
     * replication mode configured on the server. A built-in mode of
     * {@link #MAJORITY_MODE} is also supported.
     * 
     * @see <a
     *      href="http://www.mongodb.org/display/DOCS/Data+Center+Awareness">Data
     *      Center Awareness</a>
     */
    private final String myWaitForReplicasByMode;

    /**
     * True if the durability requires that the response wait for a reply from
     * the server but no special server processing.
     */
    private final boolean myWaitForReply;

    /**
     * The number of milliseconds to wait for the durability requirements to be
     * satisfied.
     */
    private final int myWaitTimeoutMillis;

    /**
     * Create a new Durability.
     * 
     * @param waitForFsync
     *            True if the durability requires that the response wait for an
     *            fsync() of the data to complete, false otherwise.
     * @param waitForJournal
     *            True if if the durability requires that the response wait for
     *            the data to be written to the server's journal, false
     *            otherwise.
     * @param waitForReplicas
     *            If the value is value greater than zero the durability
     *            requires that the response wait for the data to be received by
     *            a replica and the number of replicas of the data to wait for.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     */
    protected Durability(final boolean waitForFsync,
            final boolean waitForJournal, final int waitForReplicas,
            final int waitTimeoutMillis) {
        this(true, waitForFsync, waitForJournal, waitForReplicas, null,
                waitTimeoutMillis);
    }

    /**
     * Create a new Durability.
     * 
     * @param waitForFsync
     *            True if the durability requires that the response wait for an
     *            fsync() of the data to complete, false otherwise.
     * @param waitForJournal
     *            True if if the durability requires that the response wait for
     *            the data to be written to the server's journal, false
     *            otherwise.
     * @param waitForReplicasByMode
     *            If the value is non-<code>null</code> then wait for the
     *            specified replication mode configured on the server. A
     *            built-in mode of {@link #MAJORITY_MODE} is also supported.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     */
    protected Durability(final boolean waitForFsync,
            final boolean waitForJournal, final String waitForReplicasByMode,
            final int waitTimeoutMillis) {
        this(true, waitForFsync, waitForJournal, 0, waitForReplicasByMode,
                waitTimeoutMillis);
    }

    /**
     * Create a new Durability.
     * 
     * @param waitForReply
     *            True if the durability requires a reply from the server.
     * @param waitForFsync
     *            True if the durability requires that the response wait for an
     *            fsync() of the data to complete, false otherwise.
     * @param waitForJournal
     *            True if if the durability requires that the response wait for
     *            the data to be written to the server's journal, false
     *            otherwise.
     * @param waitForReplicas
     *            If the value is value greater than zero the durability
     *            requires that the response wait for the data to be received by
     *            a replica and the number of replicas of the data to wait for.
     * @param waitForReplicasByMode
     *            If the value is non-<code>null</code> then wait for the
     *            specified replication mode configured on the server. A
     *            built-in mode of {@link #MAJORITY_MODE} is also supported.
     * @param waitTimeoutMillis
     *            The number of milliseconds to wait for the durability
     *            requirements to be satisfied.
     */
    private Durability(final boolean waitForReply, final boolean waitForFsync,
            final boolean waitForJournal, final int waitForReplicas,
            final String waitForReplicasByMode, final int waitTimeoutMillis) {
        myWaitForReply = waitForReply;
        myWaitForFsync = waitForFsync;
        myWaitForJournal = waitForJournal;
        myWaitForReplicas = waitForReplicas;
        myWaitTimeoutMillis = waitTimeoutMillis;
        myWaitForReplicasByMode = waitForReplicasByMode;
    }

    /**
     * Returns a suitable getlasterror command's document.
     * 
     * @return The getlasterror command's document.
     */
    public Document asDocument() {
        if (myAsDocument == null) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("getlasterror", 1);
            if (isWaitForJournal()) {
                builder.addBoolean("j", true);
            }
            if (isWaitForFsync()) {
                builder.addBoolean("fsync", true);
            }
            if (getWaitTimeoutMillis() > 0) {
                builder.addInteger("wtimeout", getWaitTimeoutMillis());
            }

            if (getWaitForReplicas() >= 1) {
                builder.addInteger("w", getWaitForReplicas());
            }
            else if (getWaitForReplicasByMode() != null) {
                builder.addString("w", getWaitForReplicasByMode());
            }

            myAsDocument = new ImmutableDocument(builder);
        }
        return myAsDocument;
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
            final Durability other = (Durability) object;

            result = (myWaitForReply == other.myWaitForReply)
                    && (myWaitForFsync == other.myWaitForFsync)
                    && (myWaitForJournal == other.myWaitForJournal)
                    && (myWaitForReplicas == other.myWaitForReplicas)
                    && (myWaitTimeoutMillis == other.myWaitTimeoutMillis)
                    && nullSafeEquals(myWaitForReplicasByMode,
                            other.myWaitForReplicasByMode);
        }
        return result;
    }

    /**
     * Returns if (value greater than zero) the durability requires that the
     * response wait for the data to be received by a replica and the number of
     * replicas of the data to wait for.
     * 
     * @return If (value greater than zero) the durability requires that the
     *         response wait for the data to be received by a replica and the
     *         number of replicas of the data to wait for.
     */
    public int getWaitForReplicas() {
        return myWaitForReplicas;
    }

    /**
     * If the value is non-<code>null</code> then wait for the specified
     * replication mode configured on the server. A built-in mode of
     * {@link #MAJORITY_MODE} is also supported.
     * 
     * @return If the value is non-null then wait for the specified replication
     *         mode configured on the server.
     * @see <a
     *      href="http://www.mongodb.org/display/DOCS/Data+Center+Awareness">Data
     *      Center Awareness</a>
     */
    public String getWaitForReplicasByMode() {
        return myWaitForReplicasByMode;
    }

    /**
     * Returns the number of milliseconds to wait for the durability
     * requirements to be satisfied.
     * 
     * @return The number of milliseconds to wait for the durability
     *         requirements to be satisfied.
     */
    public int getWaitTimeoutMillis() {
        return myWaitTimeoutMillis;
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + (myWaitForReply ? 1 : 3);
        result = (31 * result) + (myWaitForFsync ? 1 : 3);
        result = (31 * result) + (myWaitForJournal ? 1 : 3);
        result = (31 * result)
                + ((myWaitForReplicasByMode != null) ? myWaitForReplicasByMode
                        .hashCode() : 3);
        result = (31 * result) + myWaitForReplicas;
        result = (31 * result) + myWaitTimeoutMillis;
        return result;
    }

    /**
     * Returns if the durability requires that the response wait for an fsync()
     * of the data on the server to complete.
     * 
     * @return True if the durability requires that the response wait for an
     *         fsync() of the data to complete, false otherwise.
     */
    public boolean isWaitForFsync() {
        return myWaitForFsync;
    }

    /**
     * Returns if the durability requires that the response wait for the data to
     * be written to the server's journal.
     * 
     * @return True if if the durability requires that the response wait for the
     *         data to be written to the server's journal, false otherwise.
     */
    public boolean isWaitForJournal() {
        return myWaitForJournal;
    }

    /**
     * Returns if the durability requires that the response wait for a reply
     * from the server but potentially no special server processing.
     * 
     * @return True if the durability requires that the response wait for a
     *         reply from the server but potentially no special server
     *         processing.
     */
    public boolean isWaitForReply() {
        return myWaitForReply;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overriden to return the durability as JSON text.
     * </p>
     */
    @Override
    public String toString() {
        String result;
        if (NONE.equals(this)) {
            result = "NONE";
        }
        else if (ACK.equals(this)) {
            result = "ACK";
        }
        else {
            // Render as a JSON Document on a single line.
            final StringWriter sink = new StringWriter();
            final JsonSerializationVisitor visitor = new JsonSerializationVisitor(
                    sink, true);
            asDocument().accept(visitor);

            result = sink.toString();
        }
        return result;
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
     * Hook into serialization to replace <tt>this</tt> object with the local
     * {@link #ACK} or {@link #NONE} instance as appropriate.
     * 
     * @return Either the {@link #ACK} or {@link #NONE} instance if
     *         <tt>this</tt> instance equals one of those instances otherwise
     *         <tt>this</tt> instance.
     */
    private Object readResolve() {
        if (this.equals(ACK)) {
            return ACK;
        }
        else if (this.equals(NONE)) {
            return NONE;
        }
        else {
            return this;
        }
    }
}
