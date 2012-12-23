/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import com.allanbank.mongodb.util.IOUtils;

/**
 * ClusterTestSupport provides a class to manage a cluster.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterTestSupport {

    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /** The working directory for the cluster. */
    private File myWorkingDirectory;

    /**
     * Creates a new ClusterTestSupport.
     */
    public ClusterTestSupport() {
    }

    /**
     * Runs a process and returns the merged stderr and stdout.
     * 
     * @param workingDirectory
     *            The working directory for the executable.
     * @param executable
     *            The program to run.
     * @param args
     *            The arguments to the executable.
     * @return The merged stdout and stderr.
     * @throws AssertionError
     *             On a failure to launch the executable.
     */
    public String run(final File workingDirectory, final String executable,
            final String... args) throws AssertionError {
        String app = executable;
        final String mongodbHome = System.getenv("MONGODB_HOME");
        if (mongodbHome != null) {
            final File mongodbHomeDir = new File(mongodbHome);
            if ("mongod".equals(executable)) {
                app = new File(new File(mongodbHomeDir, "bin"), executable)
                        .getAbsolutePath();
            }
            else if ("mongos".equals(executable)) {
                app = new File(new File(mongodbHomeDir, "bin"), executable)
                        .getAbsolutePath();
            }
            else if ("mongo".equals(executable)) {
                app = new File(new File(mongodbHomeDir, "bin"), executable)
                        .getAbsolutePath();
            }
        }

        final List<String> command = new ArrayList<String>(args.length + 1);
        command.add(app);
        command.addAll(Arrays.asList(args));

        final ProcessBuilder b = new ProcessBuilder();
        if (workingDirectory == null) {
            b.directory(myWorkingDirectory);
        }
        else {
            b.directory(workingDirectory);
        }
        b.redirectErrorStream(true);
        b.command(command);

        BufferedReader r = null;
        final StringBuilder output = new StringBuilder();
        try {
            final Process p = b.start();

            r = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line;
            while ((line = r.readLine()) != null) {
                output.append(line).append("\n");
            }
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(
                    "Could not run process: " + ioe.getMessage());
            error.initCause(ioe);

            throw error;
        }
        finally {
            IOUtils.close(r);
        }

        return output.toString();
    }

    /**
     * Starts a MongoDB instance running in a replica set mode. Below is the
     * role and port allocation.
     * <ul>
     * <li>27017 - Arbiter</li>
     * <li>27018 - Shard - Probable Primary</li>
     * <li>27019 - Shard</li>
     * <li>27020 - Shard</li>
     * </ul>
     */
    public void startReplicaSet() {
        if (myWorkingDirectory != null) {
            stopAll();
        }

        try {
            myWorkingDirectory = File.createTempFile("replica-set-", ".dir");
            myWorkingDirectory.delete();
            myWorkingDirectory.mkdir();

            startReplicaSet(myWorkingDirectory, DEFAULT_PORT, 3);
        }
        catch (final IOException ioe) {
            fail(ioe.getMessage(), ioe);
        }
    }

    /**
     * Starts a MongoDB instance running in a sharded mode. Below is the role
     * and port allocation.
     * <ul>
     * <li>27017 - mongos</li>
     * <li>27018 - mongos</li>
     * <li>27019 - config</li>
     * <li>27020 - Shard</li>
     * <li>27021 - Shard</li>
     * <li>27022 - Shard</li>
     * </ul>
     */
    public void startSharded() {
        if (myWorkingDirectory != null) {
            stopAll();
        }

        try {
            myWorkingDirectory = File.createTempFile("sharded-", ".dir");
            myWorkingDirectory.delete();
            myWorkingDirectory.mkdir();

            startSharded(myWorkingDirectory, DEFAULT_PORT, 2, 3);
        }
        catch (final IOException ioe) {
            fail(ioe.getMessage(), ioe);
        }
    }

    /**
     * Starts a MongoDB instance running in a sharded mode. Below is the role
     * and port allocation.
     * <ul>
     * <li>27017 - mongos</li>
     * <li>27018 - mongos</li>
     * <li>27019 - config</li>
     * <li>27020 - Arbiter Shard (rs-27020)</li>
     * <li>27021 - Shard (rs-27020) -- probable primary</li>
     * <li>27022 - Shard (rs-27020)</li>
     * <li>27023 - Arbiter Shard (rs-27023)</li>
     * <li>27024 - Shard (rs-27024) -- probable primary</li>
     * <li>27025 - Shard (rs-27025)</li>
     * </ul>
     */
    public void startShardedReplicaSets() {
        if (myWorkingDirectory != null) {
            stopAll();
        }

        try {
            myWorkingDirectory = File.createTempFile("sharded-", ".dir");
            myWorkingDirectory.delete();
            myWorkingDirectory.mkdir();

            startShardedReplicaSets(myWorkingDirectory, DEFAULT_PORT, 2, 2);
        }
        catch (final IOException ioe) {
            fail(ioe.getMessage(), ioe);
        }
    }

    /**
     * Starts a MongoDB instance running in a standalone mode. Below is the role
     * and port allocation.
     * <ul>
     * <li>27017 - mongod</li>
     * </ul>
     */
    public void startStandAlone() {
        if (myWorkingDirectory != null) {
            stopAll();
        }

        try {
            myWorkingDirectory = File.createTempFile("standalone-", ".dir");
            myWorkingDirectory.delete();
            myWorkingDirectory.mkdir();

            startStandAlone(myWorkingDirectory, DEFAULT_PORT);
        }
        catch (final IOException ioe) {
            fail(ioe.getMessage(), ioe);
        }
    }

    /**
     * Stops all of the running processes for the cluster.
     */
    public void stopAll() {

        run(null, "pkill", "mongod");
        run(null, "pkill", "mongos");

        if (myWorkingDirectory != null) {
            run(myWorkingDirectory, "pkill", "-f",
                    myWorkingDirectory.getAbsolutePath() + ".*");
            run(myWorkingDirectory.getParentFile(), "rm", "-rf",
                    myWorkingDirectory.getAbsolutePath());
        }

        myWorkingDirectory = null;
    }

    /**
     * Fails with the message and exception.
     * 
     * @param message
     *            The failure message.
     * @throws AssertionError
     *             Always.
     */
    protected void fail(final String message) throws AssertionError {
        Assert.fail(message);
    }

    /**
     * Fails with the message and exception.
     * 
     * @param message
     *            The failure message.
     * @param cause
     *            The failure cause.
     * @throws AssertionError
     *             Always.
     */
    protected void fail(final String message, final Throwable cause)
            throws AssertionError {
        final AssertionError error = new AssertionError(message);
        error.initCause(cause);

        throw error;
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
     * Starts a replica set on the specified start port.
     * 
     * @param workingDirectory
     *            The work directory for the replica set.
     * @param startPort
     *            The starting port for the replica set.
     * @param replicas
     *            The number of replicas in the replica set.
     * @throws AssertionError
     *             On a failure starting the replica set.
     */
    protected void startReplicaSet(final File workingDirectory,
            final int startPort, final int replicas) throws AssertionError {

        final File rsConfig = new File(workingDirectory, "config-" + startPort
                + ".js");
        FileWriter writer = null;
        try {
            writer = new FileWriter(rsConfig);
            writer.write("rs.initiate({ _id: \"rs-" + startPort
                    + "\", members: [\n");

            // Arbiter.
            int port = startPort;
            File log = new File(workingDirectory, "mongod-" + port + ".log");
            File db = new File(workingDirectory, "mongod-" + port);
            db.mkdir();
            run(workingDirectory, "mongod", "--port", String.valueOf(port),
                    "--fork", "--dbpath", db.getAbsolutePath(), "--smallfiles",
                    "--logpath", log.getAbsolutePath(), "--replSet", "rs-"
                            + startPort, "--noprealloc", "--nojournal",
                    "--oplogSize", "2");
            waitFor(log, port, TimeUnit.SECONDS.toMillis(30));
            writer.write("{ _id: 0, host: \"localhost:" + port
                    + "\", arbiterOnly:true }");

            for (int i = 0; i < replicas; ++i) {
                port = startPort + i + 1;
                log = new File(workingDirectory, "mongod-" + port + ".log");
                db = new File(workingDirectory, "mongod-" + port);
                db.mkdir();

                run(workingDirectory, "mongod", "--port", String.valueOf(port),
                        "--fork", "--dbpath", db.getAbsolutePath(),
                        "--smallfiles", "--logpath", log.getAbsolutePath(),
                        "--replSet", "rs-" + startPort, "--noprealloc",
                        "--nojournal", "--oplogSize", "512");
                waitFor(log, port, TimeUnit.SECONDS.toMillis(30));
                writer.write(",\n  { _id: " + (i + 1) + ", host: \"localhost:"
                        + port + "\" }");

            }
            writer.write("\n] })");
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }

        run(workingDirectory, "mongo",
                "localhost:" + String.valueOf(startPort + 1) + "/admin",
                rsConfig.getAbsolutePath());

        final File log = new File(workingDirectory, "mongod-" + startPort
                + ".log");
        waitFor(log, "is now in state PRIMARY", TimeUnit.MINUTES.toMillis(3));

        // Each replica will be a secondary at some point.
        waitFor(log, "is now in state SECONDARY", replicas,
                TimeUnit.MINUTES.toMillis(3));

        // Wait for everything to be calm.
        sleep(TimeUnit.SECONDS.toMillis(2));
    }

    /**
     * Starts a sharded cluster on the specified start port.
     * 
     * @param workingDirectory
     *            The work directory for the shards.
     * @param startPort
     *            The starting port for the shards.
     * @param mongos
     *            The number of mongos servers to start.
     * @param shards
     *            The number of shards to start.
     * @throws AssertionError
     *             On a failure starting the cluster.
     */
    protected void startSharded(final File workingDirectory,
            final int startPort, final int mongos, final int shards)
            throws AssertionError {

        final File shardsConfig = new File(workingDirectory, "shards-"
                + startPort + ".js");
        FileWriter writer = null;
        try {
            writer = new FileWriter(shardsConfig);

            final int configPort = startPort + mongos;
            final File configLog = new File(workingDirectory, "config-"
                    + configPort + ".log");
            final File configDb = new File(workingDirectory, "config-"
                    + configPort);
            configDb.mkdir();

            run(workingDirectory, "mongod", "--configsvr", "--port",
                    String.valueOf(configPort), "--fork", "--dbpath",
                    configDb.getAbsolutePath(), "--logpath",
                    configLog.getAbsolutePath(), "--nojournal");
            waitFor(configLog, configPort, TimeUnit.SECONDS.toMillis(30));

            for (int i = 0; i < mongos; ++i) {
                final int port = startPort + i;
                final File log = new File(workingDirectory, "mongos-" + port
                        + ".log");

                run(workingDirectory, "mongos", "--port", String.valueOf(port),
                        "--fork", "--logpath", log.getAbsolutePath(),
                        "--configdb", "localhost:" + configPort);
                waitFor(log, port, TimeUnit.SECONDS.toMillis(30));
            }

            for (int i = 0; i < shards; ++i) {
                final int port = startPort + i + mongos + 1;
                final File log = new File(workingDirectory, "mongod-" + port
                        + ".log");
                final File db = new File(workingDirectory, "mongod-" + port);
                db.mkdir();

                run(workingDirectory, "mongod", "--shardsvr", "--port",
                        String.valueOf(port), "--fork", "--dbpath",
                        db.getAbsolutePath(), "--smallfiles", "--logpath",
                        log.getAbsolutePath(), "--noprealloc", "--nojournal");
                waitFor(log, port, TimeUnit.SECONDS.toMillis(30));
                writer.write("db.runCommand( { addshard : \"localhost:" + port
                        + "\" } );\n");

            }
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }

        // Add all of the shards.
        run(workingDirectory, "mongo", "localhost:" + String.valueOf(startPort)
                + "/admin", shardsConfig.getAbsolutePath());

        // Wait for everything to be calm.
        sleep(TimeUnit.SECONDS.toMillis(2));
    }

    /**
     * Starts a sharded cluster on the specified start port.
     * 
     * @param workingDirectory
     *            The work directory for the shards.
     * @param startPort
     *            The starting port for the shards.
     * @param mongos
     *            The number of mongos servers to start.
     * @param shards
     *            The number of shards to start.
     * @throws AssertionError
     *             On a failure starting the cluster.
     */
    protected void startShardedReplicaSets(final File workingDirectory,
            final int startPort, final int mongos, final int shards)
            throws AssertionError {

        final File shardsConfig = new File(workingDirectory, "shards-"
                + startPort + ".js");
        FileWriter writer = null;
        try {
            writer = new FileWriter(shardsConfig);

            final int configPort = startPort + mongos;
            final File configLog = new File(workingDirectory, "config-"
                    + configPort + ".log");
            final File configDb = new File(workingDirectory, "config-"
                    + configPort);
            configDb.mkdir();

            run(workingDirectory, "mongod", "--configsvr", "--port",
                    String.valueOf(configPort), "--fork", "--dbpath",
                    configDb.getAbsolutePath(), "--logpath",
                    configLog.getAbsolutePath(), "--nojournal");
            waitFor(configLog, configPort, TimeUnit.SECONDS.toMillis(30));

            for (int i = 0; i < mongos; ++i) {
                final int port = startPort + i;
                final File log = new File(workingDirectory, "mongos-" + port
                        + ".log");

                run(workingDirectory, "mongos", "--port", String.valueOf(port),
                        "--fork", "--logpath", log.getAbsolutePath(),
                        "--configdb", "localhost:" + configPort);
                waitFor(log, port, TimeUnit.SECONDS.toMillis(30));
            }

            final int rsStartPort = startPort + mongos + 1;
            for (int i = 0; i < shards; ++i) {
                final int port = rsStartPort + (i * 3);

                startReplicaSet(workingDirectory, port, 2);

                writer.write("db.runCommand( { addshard : \"" + "rs-" + port
                        + "/localhost:" + port + "\" } );\n");
            }
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }

        // Add all of the shards.
        run(workingDirectory, "mongo", "localhost:" + String.valueOf(startPort)
                + "/admin", shardsConfig.getAbsolutePath());

        // Wait for everything to be calm.
        sleep(TimeUnit.SECONDS.toMillis(2));
    }

    /**
     * Starts a mongod on the specified port.
     * 
     * @param workingDirectory
     *            The work directory for the mongod.
     * @param port
     *            The port for the mongod.
     * @throws AssertionError
     *             On a failure starting the cluster.
     */
    protected void startStandAlone(final File workingDirectory, final int port)
            throws AssertionError {

        final File log = new File(workingDirectory, "mongod-" + port + ".log");
        final File db = new File(workingDirectory, "mongod-" + port);
        db.mkdir();

        run(workingDirectory, "mongod", "--port", String.valueOf(port),
                "--fork", "--dbpath", db.getAbsolutePath(), "--smallfiles",
                "--logpath", log.getAbsolutePath(), "--noprealloc",
                "--nojournal");
        waitFor(log, port, TimeUnit.SECONDS.toMillis(30));

        // Wait for everything to be calm.
        sleep(TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Waits for the log file to contain the standard message that mongod is
     * waiting on the specified port.
     * 
     * @param log
     *            The log file to scan.
     * @param port
     *            The port to search for.
     * @param waitMs
     *            How long to wait before giving up.
     */
    protected void waitFor(final File log, final int port, final long waitMs) {
        waitFor(log, "waiting for connections on port " + port, 1, waitMs);
    }

    /**
     * Waits for the log file to contain the specified token {@code count}
     * times.
     * 
     * @param log
     *            The log file to scan.
     * @param token
     *            The token to search for.
     * @param count
     *            The number of instances of the token to find.
     * @param waitMs
     *            How long to wait before giving up.
     */
    protected void waitFor(final File log, final String token, final int count,
            final long waitMs) {

        int seen = 0;
        final long now = System.currentTimeMillis();
        final long deadline = now + waitMs;
        while (now < deadline) {
            seen = 0;
            BufferedReader r = null;
            try {
                r = new BufferedReader(new FileReader(log));

                String line = null;
                while ((line = r.readLine()) != null) {
                    int offset = 0;
                    while (line.indexOf(token, offset) >= 0) {
                        offset = (line.indexOf(token, offset) + token.length());
                        seen += 1;
                    }
                }
            }
            catch (final IOException ioe) {
                fail("Could not read the log file.", ioe);
            }
            finally {
                IOUtils.close(r);
            }

            if (seen >= count) {
                return;
            }

            sleep(100);
        }

        throw new AssertionError("Did not find '" + token + "' in the log '"
                + log.getAbsolutePath() + "' '" + count + "' times.  Only '"
                + seen + "' times.");
    }

    /**
     * Waits for the log file to contain the specified token.
     * 
     * @param log
     *            The log file to scan.
     * @param token
     *            The token to search for.
     * @param waitMs
     *            How long to wait before giving up.
     */
    protected void waitFor(final File log, final String token, final long waitMs) {
        waitFor(log, token, 1, waitMs);
    }
}
