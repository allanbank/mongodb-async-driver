/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ManagedProcess provides the ability to manage a process.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ManagedProcess {

    /** The log for the process. */
    protected final Lock myLock;

    /** The output of the process. */
    protected final StringBuilder myOutput;

    /** The reader for data from the process. */
    protected final BufferedReader myReader;

    /** The process being managed. */
    private final Process myProcess;

    /**
     * Creates a new ClusterTestSupport.ManagedProcess.
     * 
     * @param executable
     *            The executable being run.
     * @param process
     *            The process to manage.
     */
    public ManagedProcess(final String executable, final Process process) {
        myProcess = process;
        myOutput = new StringBuilder();
        myLock = new ReentrantLock();

        myReader = new BufferedReader(new InputStreamReader(
                myProcess.getInputStream()));

        new Thread(new OutputReader(), executable + " Process Drain").start();
    }

    /**
     * Closes/kills the managed process.
     */
    public void close() {
        myProcess.destroy();
    }

    /**
     * Returns the output value.
     * 
     * @return The output value.
     */
    public String getOutput() {
        myLock.lock();
        try {
            return myOutput.toString();
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * Waits for the process to exit.
     */
    public void waitFor() {
        try {
            myProcess.waitFor();
        }
        catch (final InterruptedException ie) {
            final AssertionError error = new AssertionError(
                    "Failed to wait for the process to exit gracefully.");
            error.initCause(ie);

            throw error;
        }
    }

    /**
     * Waits for the log file to contain the standard message that mongod is
     * waiting on the specified port.
     * 
     * @param port
     *            The port to search for.
     * @param waitMs
     *            How long to wait before giving up.
     */
    public void waitFor(final int port, final long waitMs) {
        waitFor("waiting for connections on port " + port, 1, waitMs);
    }

    /**
     * Waits for the log file to contain the specified token {@code count}
     * times.
     * 
     * @param token
     *            The token to search for.
     * @param count
     *            The number of instances of the token to find.
     * @param waitMs
     *            How long to wait before giving up.
     */
    public void waitFor(final String token, final int count, final long waitMs) {

        int seen = 0;
        long now = System.currentTimeMillis();
        final long deadline = now + waitMs;
        while (now < deadline) {
            seen = 0;

            final String line = getOutput();
            int offset = 0;
            while (line.indexOf(token, offset) >= 0) {
                offset = (line.indexOf(token, offset) + token.length());
                seen += 1;
            }

            if (seen >= count) {
                return;
            }

            if (!hasGrown(line.length())) {
                sleep(100);
            }

            now = System.currentTimeMillis();
        }

        throw new AssertionError("Did not find '" + token + "' in the output '"
                + count + "' times.  Only '" + seen + "' times: " + getOutput());
    }

    /**
     * Waits for the log file to contain the specified token.
     * 
     * @param token
     *            The token to search for.
     * @param waitMs
     *            How long to wait before giving up.
     */
    public void waitFor(final String token, final long waitMs) {
        waitFor(token, 1, waitMs);
    }

    /**
     * Sleeps for the specified number of milliseconds.
     * 
     * @param millis
     *            The number of milliseconds to sleep.
     */
    protected void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (final InterruptedException ie) {
            // Ignore;
        }
    }

    /**
     * Returns true if the output buffer has grown from the length.
     * 
     * @param length
     *            The length for the buffer.
     * @return True if the buffer contains more characters than {@code length}.
     */
    private boolean hasGrown(final int length) {
        myLock.lock();
        try {
            return (myOutput.length() > length);
        }
        finally {
            myLock.unlock();
        }
    }

    /**
     * OutputReader provides a background process to read in all of the output
     * from the process.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class OutputReader implements Runnable {
        @Override
        public void run() {
            try {
                final char[] buffer = new char[1024];
                while (true) {
                    final int read = myReader.read(buffer);
                    if (read > 0) {
                        myLock.lock();
                        try {
                            myOutput.append(buffer, 0, read);
                        }
                        finally {
                            myLock.unlock();
                        }
                    }
                    else if (read < 0) {
                        // EOF.
                        return;
                    }
                }
            }
            catch (final IOException ioe) {
                // Just exit.
            }
        }
    }
}