/*
 * #%L
 * ManagedProcess.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ManagedProcess provides the ability to manage a process.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ManagedProcess {

    /**
     * Boolean to control if the output from the MongoDB processes are written
     * to the console. This is normally false but can be turned on with a system
     * property or, more likely, at the start of a test (hence no final
     * modifier).
     */
    public static boolean ourWriteMongoDbOutput = Boolean
            .getBoolean("write.mongodb.output");

    /** The log for the process. */
    protected final Lock myLock;

    /** The condition to notify listeners that the log has been updated. */
    protected final Condition myLogUpdated;

    /** The output of the process. */
    protected final StringBuilder myOutput;

    /** The reader for data from the process. */
    protected final BufferedReader myReader;

    /** The process being managed. */
    private final Process myProcess;

    private final String myPortNumber;

    /**
     * Creates a new ClusterTestSupport.ManagedProcess.
     *
     * @param executable
     *            The executable being run.
     * @param process
     *            The process to manage.
     */
    public ManagedProcess(final String executable, final Process process, String portNumber) {
        myPortNumber = portNumber;
        myProcess = process;
        myOutput = new StringBuilder();

        myLock = new ReentrantLock();
        myLogUpdated = myLock.newCondition();

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
            String output = myOutput.toString();
            return output;
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
     * Waits for the specified port to start accepting connections.
     *
     * @param port
     *            The port to search for.
     * @param waitMs
     *            How long to wait before giving up.
     */
    public void waitFor(final int port, final long waitMs) {

        final InetSocketAddress connectTo = new InetSocketAddress(port);

        long now = System.currentTimeMillis();
        final long deadline = now + waitMs;
        while (now < deadline) {
            final Socket socket = new Socket();
            try {
                socket.connect(connectTo);

                // Excellent - Done waiting.
                return;
            }
            catch (final IOException okTryAgain) {
                // Try again.
                sleep(1);
            }
            finally {
                try {
                    socket.close();
                }
                catch (final IOException oops) {
                    // Ignore.
                }
            }
            now = System.currentTimeMillis();
        }

        throw new AssertionError("Socket on port '" + port
                + "' did not open within the expected time (" + waitMs
                + " ms).\n" + getOutput());
    }

    /**
     * Waits for the log file to contain the specified token {@code count}
     * times.
     *
     * @param tokenRegex
     *            The token regular expression to search for.
     * @param count
     *            The number of instances of the token to find.
     * @param waitMs
     *            How long to wait before giving up.
     */
    public void waitFor(final String tokenRegex, final int count,
            final long waitMs) {

        int seen = 0;
        long now = System.currentTimeMillis();
        final long deadline = now + waitMs;
        final Pattern pattern = Pattern.compile(tokenRegex);
        while (now < deadline) {
            seen = 0;

            myLock.lock();
            try {
                final String line = getOutput();
                final Matcher matcher = pattern.matcher(line);
                int offset = 0;
                while (matcher.find(offset)) {
                    offset = matcher.end();
                    seen += 1;
                }

                if (seen >= count) {
                    return;
                }

                if (!hasGrown(line.length())) {
                    myLogUpdated.await(deadline - now, TimeUnit.MILLISECONDS);
                }
            }
            catch (final InterruptedException e) {
                // Spurious wake-up. Ignore.
            }
            finally {
                myLock.unlock();
            }

            now = System.currentTimeMillis();
        }

        throw new AssertionError("Did not find '" + tokenRegex
                + "' in the output '" + count + "' times.  Only '" + seen
                + "' times: " + getOutput());
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
    protected final class OutputReader
            implements Runnable {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            String prefix = "[" + Thread.currentThread().getId()+ "] [" + myPortNumber + "]";
            try {
                final char[] buffer = new char[1024];
                while (true) {
                    myReader.readLine();
                    final int read = myReader.read(buffer);
                    if (read > 0) {
                        myLock.lock();
                        try {
                            if (ourWriteMongoDbOutput) {
                                System.out.print(prefix + new String(buffer, 0, read));
                            }
                            myOutput.append(buffer, 0, read);
                            myLogUpdated.signalAll();
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