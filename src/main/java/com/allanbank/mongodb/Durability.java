/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

/**
 * Represents the required durability of writes (inserts, updates, and deletes)
 * on the server.
 * <ul>
 * <li>The lowest durability ({@link #NONE}) has no guarantees that the data
 * will survive a catastrophic server failure.
 * <li>The next level of durability ({@link #journalDurable(int)}) ensures that
 * the data is written to the server's journal before returning.
 * <li>Next level ({@link #fsyncDurable(int)}) is to ensure that the data has
 * been fsync()'d to the server's disk.
 * <li>For the highest level of durability ({@link #replicaDurable(int)}), the
 * server ensure that the data has been received by 1 to
 * {@link #replicaDurable(int, int) N} replicas in the replica set.</li>
 * </ul>
 * <p>
 * Generally, increasing the level of durability decreases performance.
 * </p>
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Durability {

    /** The durability that says no durability is required. */
    public final static Durability NONE = new Durability(false, false, 0, 0);

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
     * Returns the none value.
     * 
     * @return the none
     */
    public static Durability getNone() {
        return NONE;
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
    private Durability(final boolean waitForFsync,
            final boolean waitForJournal, final int waitForReplicas,
            final int waitTimeoutMillis) {
        super();
        myWaitForFsync = waitForFsync;
        myWaitForJournal = waitForJournal;
        myWaitForReplicas = waitForReplicas;
        myWaitTimeoutMillis = waitTimeoutMillis;
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

            result = (myWaitForFsync == other.myWaitForFsync)
                    && (myWaitForJournal == other.myWaitForJournal)
                    && (myWaitForReplicas == other.myWaitForReplicas)
                    && (myWaitTimeoutMillis == other.myWaitTimeoutMillis);
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
        result = (31 * result) + super.hashCode();
        result = (31 * result) + (myWaitForFsync ? 1 : 3);
        result = (31 * result) + (myWaitForJournal ? 1 : 3);
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

}
