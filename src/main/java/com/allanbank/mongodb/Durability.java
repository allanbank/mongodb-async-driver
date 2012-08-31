/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Serializable;

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
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Durability implements Serializable {

    /** The durability that says no durability is required. */
    public final static Durability ACK = new Durability(true, false, false, 0,
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
     * {@link #ACK} or {@link #NONE} instance as appropriate..
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
