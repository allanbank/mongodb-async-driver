/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
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

    /** The suffix for the test directories. */
    public static final String DIR_SUFFIX = "-test-dir";

    /** The prefix for replica set test directories. */
    public static final String REPLICA_SET_ROOT = "replica-set-";

    /** The prefix for sharded test directories. */
    public static final String SHARDED_ROOT = "sharded-";

    /** The prefix for standalone test directories. */
    public static final String STANDALONE_ROOT = "standalone-";

    /** The processes we have started. */
    private static Boolean ourSupportsText;

    /** The processes we have started. */
    private final List<ManagedProcess> myProcesses;

    /** The working directory for the cluster. */
    private File myWorkingDirectory;

    /**
     * Creates a new ClusterTestSupport.
     */
    public ClusterTestSupport() {
        myProcesses = new ArrayList<ManagedProcess>();
    }

    /**
     * Deletes the files.
     * 
     * @param file
     *            The file to delete. Will delete all sub directories and files
     *            if a directory.
     */
    public void delete(final File file) {
        if (file.isDirectory()) {
            for (final File child : file.listFiles()) {
                delete(child);
            }
        }

        file.delete();
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
     * @return The managed process.
     * @throws AssertionError
     *             On a failure to launch the executable.
     */
    public ManagedProcess run(final File workingDirectory,
            final String executable, final String... args)
            throws AssertionError {

        // Need a parameter to turn on text search in 2.4.
        final List<String> origArgs = Arrays.asList(args);
        List<String> augmentedArgs = origArgs;

        final Boolean supports = ourSupportsText;
        if (((supports == null) || supports.booleanValue())
                && ("mongod".equals(executable) || "mongos".equals(executable))) {
            augmentedArgs = new ArrayList<String>(origArgs);
            augmentedArgs.add("--setParameter");
            augmentedArgs.add("textSearchEnabled=1");
        }

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

        final List<String> command = new ArrayList<String>(
                augmentedArgs.size() + 1);
        command.add(app);
        command.addAll(augmentedArgs);

        final ProcessBuilder b = new ProcessBuilder();
        if (workingDirectory == null) {
            b.directory(myWorkingDirectory);
        }
        else {
            b.directory(workingDirectory);
        }
        b.redirectErrorStream(true);
        b.command(command);

        final BufferedReader r = null;
        try {
            ManagedProcess mp = new ManagedProcess(executable, b.start());

            // If we tried to turn on text search in 2.3 it will fail...
            if (origArgs != augmentedArgs) {
                if (supports == null) {
                    sleep(100);
                    if (mp.getOutput().contains("error command line")) {
                        ourSupportsText = Boolean.FALSE;

                        // Shut down the old...
                        mp.close();

                        // Just use the original args.
                        command.clear();
                        command.add(app);
                        command.addAll(origArgs);
                        b.command(command);

                        mp = new ManagedProcess(executable, b.start());
                    }
                    else {
                        ourSupportsText = Boolean.TRUE;
                    }
                }
            }
            return mp;
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
            myWorkingDirectory = File.createTempFile(REPLICA_SET_ROOT,
                    DIR_SUFFIX);
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
            myWorkingDirectory = File.createTempFile(SHARDED_ROOT, DIR_SUFFIX);
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
            myWorkingDirectory = File.createTempFile(SHARDED_ROOT, DIR_SUFFIX);
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
            myWorkingDirectory = File.createTempFile(STANDALONE_ROOT,
                    DIR_SUFFIX);
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

        for (final ManagedProcess process : myProcesses) {
            process.close();
        }

        if (myWorkingDirectory != null) {
            delete(myWorkingDirectory);

            // Delete any strangler directories.
            final File parent = myWorkingDirectory.getParentFile();
            for (final File child : parent
                    .listFiles(new TestDirectoryFilenameFilter())) {
                delete(child);
            }
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
        ManagedProcess arbiter = null;
        try {
            writer = new FileWriter(rsConfig);
            writer.write("rs.initiate({ _id: \"rs-" + startPort
                    + "\", members: [\n");

            // Arbiter.
            int port = startPort;
            File db = new File(workingDirectory, "mongod-" + port);
            db.mkdir();
            arbiter = run(workingDirectory, "mongod", "--port",
                    String.valueOf(port), "--dbpath", db.getAbsolutePath(),
                    "--smallfiles", "--replSet", "rs-" + startPort,
                    "--noprealloc", "--nojournal", "--oplogSize", "2");
            myProcesses.add(arbiter);

            arbiter.waitFor(port, TimeUnit.SECONDS.toMillis(30));
            writer.write("{ _id: 0, host: \"localhost:" + port
                    + "\", arbiterOnly:true }");

            for (int i = 0; i < replicas; ++i) {
                port = startPort + i + 1;
                db = new File(workingDirectory, "mongod-" + port);
                db.mkdir();

                final ManagedProcess member = run(workingDirectory, "mongod",
                        "--port", String.valueOf(port), "--dbpath",
                        db.getAbsolutePath(), "--smallfiles", "--replSet",
                        "rs-" + startPort, "--noprealloc", "--nojournal",
                        "--oplogSize", "512");
                myProcesses.add(member);

                member.waitFor(port, TimeUnit.SECONDS.toMillis(30));
                writer.write(",\n  { _id: " + (i + 1) + ", host: \"localhost:"
                        + port + "\" }");

            }

            writer.write("\n] })");
            IOUtils.close(writer);

            final ManagedProcess config = run(workingDirectory, "mongo",
                    "localhost:" + String.valueOf(startPort + 1) + "/admin",
                    rsConfig.getAbsolutePath());
            config.waitFor();

            arbiter.waitFor("is now in state PRIMARY",
                    TimeUnit.MINUTES.toMillis(3));

            // Each replica will be a secondary at some point.
            arbiter.waitFor("is now in state SECONDARY", replicas,
                    TimeUnit.MINUTES.toMillis(3));

            // Wait for everything to be calm.
            sleep(TimeUnit.SECONDS.toMillis(2));
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }
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
            final File configDb = new File(workingDirectory, "config-"
                    + configPort);
            configDb.mkdir();

            final ManagedProcess config = run(workingDirectory, "mongod",
                    "--configsvr", "--port", String.valueOf(configPort),
                    "--dbpath", configDb.getAbsolutePath(), "--nojournal");
            myProcesses.add(config);
            config.waitFor(configPort, TimeUnit.SECONDS.toMillis(30));

            for (int i = 0; i < mongos; ++i) {
                final int port = startPort + i;

                final ManagedProcess mongosProcesss = run(workingDirectory,
                        "mongos", "--port", String.valueOf(port), "--configdb",
                        "localhost:" + configPort);
                myProcesses.add(mongosProcesss);
                mongosProcesss.waitFor(port, TimeUnit.SECONDS.toMillis(30));
            }

            for (int i = 0; i < shards; ++i) {
                final int port = startPort + i + mongos + 1;
                final File db = new File(workingDirectory, "mongod-" + port);
                db.mkdir();

                final ManagedProcess shard = run(workingDirectory, "mongod",
                        "--shardsvr", "--port", String.valueOf(port),
                        "--dbpath", db.getAbsolutePath(), "--smallfiles",
                        "--noprealloc", "--nojournal");
                myProcesses.add(shard);
                shard.waitFor(port, TimeUnit.SECONDS.toMillis(30));
                writer.write("db.runCommand( { addshard : \"localhost:" + port
                        + "\" } );\n");
            }

            IOUtils.close(writer);

            // Add all of the shards.
            final ManagedProcess init = run(workingDirectory, "mongo",
                    "localhost:" + String.valueOf(startPort) + "/admin",
                    shardsConfig.getAbsolutePath());
            init.waitFor();

            // Wait for everything to be calm.
            sleep(TimeUnit.SECONDS.toMillis(2));
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }

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
            final File configDb = new File(workingDirectory, "config-"
                    + configPort);
            configDb.mkdir();

            final ManagedProcess config = run(workingDirectory, "mongod",
                    "--configsvr", "--port", String.valueOf(configPort),
                    "--dbpath", configDb.getAbsolutePath(), "--nojournal");
            myProcesses.add(config);

            config.waitFor(configPort, TimeUnit.SECONDS.toMillis(30));

            for (int i = 0; i < mongos; ++i) {
                final int port = startPort + i;

                final ManagedProcess mongosProcess = run(workingDirectory,
                        "mongos", "--port", String.valueOf(port), "--configdb",
                        "localhost:" + configPort);
                myProcesses.add(mongosProcess);

                mongosProcess.waitFor(port, TimeUnit.SECONDS.toMillis(30));
            }

            final int rsStartPort = startPort + mongos + 1;
            for (int i = 0; i < shards; ++i) {
                final int port = rsStartPort + (i * 3);

                startReplicaSet(workingDirectory, port, 2);

                writer.write("db.runCommand( { addshard : \"" + "rs-" + port
                        + "/localhost:" + port + "\" } );\n");
            }
            IOUtils.close(writer);

            // Add all of the shards.
            final ManagedProcess init = run(workingDirectory, "mongo",
                    "localhost:" + String.valueOf(startPort) + "/admin",
                    shardsConfig.getAbsolutePath());
            init.waitFor();

            // Wait for everything to be calm.
            sleep(TimeUnit.SECONDS.toMillis(2));
        }
        catch (final IOException ioe) {
            fail("Could not write the replica set config.", ioe);
        }
        finally {
            IOUtils.close(writer);
        }
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

        final File db = new File(workingDirectory, "mongod-" + port);
        db.mkdir();

        final ManagedProcess standalone = run(workingDirectory, "mongod",
                "--port", String.valueOf(port), "--dbpath",
                db.getAbsolutePath(), "--smallfiles", "--noprealloc",
                "--nojournal");
        myProcesses.add(standalone);

        standalone.waitFor(port, TimeUnit.SECONDS.toMillis(30));

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

    /**
     * TestDirectoryFilenameFilter provides a file name filter to locate test
     * directories left behind.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final static class TestDirectoryFilenameFilter implements
            FilenameFilter {
        @Override
        public boolean accept(final File dir, final String name) {
            return name.endsWith(DIR_SUFFIX)
                    && (name.startsWith(STANDALONE_ROOT)
                            || name.startsWith(REPLICA_SET_ROOT) || name
                                .startsWith(SHARDED_ROOT));
        }
    }
}
