package com.allanbank.mongodb.client.transport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.MongoDbException;

/**
 * A test {@link TransportResponseListener}.
 *
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class TestTransportResponseListener
        implements TransportResponseListener {

    /** The set of received responses. */
    private final List<MongoDbException> myCloses;

    /** The set of received responses. */
    private final List<TransportInputBuffer> myResponses;

    /**
     * Creates a new TestTransportResponseListener.
     */
    public TestTransportResponseListener() {
        myResponses = new CopyOnWriteArrayList<TransportInputBuffer>();
        myCloses = new CopyOnWriteArrayList<MongoDbException>();
    }

    /**
     * Collects the close exceptions.
     *
     * @param error
     *            The close exception.
     */
    @Override
    public void closed(final MongoDbException error) {
        synchronized (this) {
            myCloses.add(error);
            notifyAll();
        }
    }

    /**
     * Returns the close exceptions received.
     *
     * @return The close exceptions received.
     */
    public List<MongoDbException> getCloses() {
        return myCloses;
    }

    /**
     * Returns the responses received.
     *
     * @return The responses received.
     */
    public List<TransportInputBuffer> getResponses() {
        return myResponses;
    }

    /**
     * Collects the responses.
     *
     * @param buffer
     *            The response.
     */
    @Override
    public void response(final TransportInputBuffer buffer) {
        synchronized (this) {
            myResponses.add(buffer);
            notifyAll();
        }
    }

    /**
     * Waits for a close up to the max time allowed.
     *
     * @param closeCount
     *            The number of responses to wait for.
     * @param maxTime
     *            The maximum amount of time to wait.
     * @param units
     *            The units for the amount of time to wait.
     */
    public void waitForClose(final int closeCount, final int maxTime,
            final TimeUnit units) {
        long now = System.currentTimeMillis();
        final long deadline = now + units.toMillis(maxTime);

        synchronized (this) {
            while ((myCloses.size() < closeCount) && (now < deadline)) {
                try {
                    wait(deadline - now);
                }
                catch (final InterruptedException e) {
                    // Ignore.
                    e.hashCode();
                }
                now = System.currentTimeMillis();
            }
        }
    }

    /**
     * Waits for a close up to the max time allowed.
     *
     * @param maxTime
     *            The maximum amount of time to wait.
     * @param units
     *            The units for the amount of time to wait.
     */
    public void waitForClose(final int maxTime, final TimeUnit units) {
        waitForClose(1, maxTime, units);
    }

    /**
     * Waits for a close up to the max time allowed.
     *
     * @param maxTime
     *            The maximum amount of time to wait.
     * @param units
     *            The units for the amount of time to wait.
     */
    public void waitForResponse(final int maxTime, final TimeUnit units) {
        waitForResponses(1, maxTime, units);
    }

    /**
     * Waits for a close up to the max time allowed.
     *
     * @param responseCount
     *            The number of responses to wait for.
     * @param maxTime
     *            The maximum amount of time to wait.
     * @param units
     *            The units for the amount of time to wait.
     */
    public void waitForResponses(final int responseCount, final int maxTime,
            final TimeUnit units) {
        long now = System.currentTimeMillis();
        final long deadline = now + units.toMillis(maxTime);

        synchronized (this) {
            while ((myResponses.size() < responseCount) && (now < deadline)) {
                try {
                    wait(deadline - now);
                }
                catch (final InterruptedException e) {
                    // Ignore.
                    e.hashCode();
                }
                now = System.currentTimeMillis();
            }
        }
    }

}