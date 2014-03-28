/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.util.IOUtils;

/**
 * JulLogTest provides tests for the {@link Slf4jLog} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Slf4jLogTest {

    /** The SLF4J {@link LogFactory#getLog(Class)} method. */
    private static Method ourFactoryMethod = null;

    /** The SLF4J API jar. */
    private static File ourSlf4jApiJar = null;

    /** The SLF4J over JUL jar. */
    private static File ourSlf4jOverJulJar = null;

    /**
     * Downloads the SLF4J API jar.
     */
    @BeforeClass
    public static void bootSlf4j() {
        final String version = "1.7.6";

        ourSlf4jApiJar = download("slf4j-api", version);
        ourSlf4jOverJulJar = download("slf4j-jdk14", version);

        try {
            // Inject the SLF4J jars into the system class loader.
            final ClassLoader systemLoader = ClassLoader.getSystemClassLoader();
            assumeThat(systemLoader, instanceOf(URLClassLoader.class));

            final Method addUrlMethod = (URLClassLoader.class
                    .getDeclaredMethod("addURL", URL.class));
            addUrlMethod.setAccessible(true);

            addUrlMethod.invoke(systemLoader, ourSlf4jApiJar.toURI().toURL());
            addUrlMethod.invoke(systemLoader, ourSlf4jOverJulJar.toURI()
                    .toURL());

            final Class<?> clazz = systemLoader
                    .loadClass("com.allanbank.mongodb.util.log.Slf4jLogFactory");
            ourFactoryMethod = clazz.getDeclaredMethod("doGetLog", Class.class);
            ourFactoryMethod.setAccessible(true);
        }
        catch (final Exception e) {
            e.printStackTrace();
            assumeNoException(e);
        }
    }

    /**
     * Deletes the downloaded SLF4J Jar.
     */
    @AfterClass
    public static void deleteJar() {
        if (ourSlf4jApiJar != null) {
            ourSlf4jApiJar.delete();
            ourSlf4jApiJar = null;
        }
    }

    /**
     * Downloads the specified jar.
     * 
     * @param name
     *            The name of the jar to download.
     * @param version
     *            The version of the jar to download.
     * @return The jar file location.
     */
    private static File download(final String name, final String version) {
        FileOutputStream out = null;
        InputStream in = null;
        File file = null;
        try {
            final URL url = new URL(
                    "http://search.maven.org/remotecontent?filepath=org/slf4j/"
                            + name + "/" + version + "/" + name + "-" + version
                            + ".jar");

            file = File.createTempFile(name, ".jar");
            file.deleteOnExit();

            out = new FileOutputStream(file);
            in = url.openStream();

            final byte[] buffer = new byte[4096];
            int read = in.read(buffer);
            while (read >= 0) {
                out.write(buffer, 0, read);

                read = in.read(buffer);
            }

            out.flush();
        }
        catch (final Exception e) {
            assumeNoException(e);
        }
        finally {
            IOUtils.close(out);
            IOUtils.close(in);
        }

        return file;
    }

    /** The handler to capture the log records. */
    private CaptureHandler myCaptureHandler = null;

    /** The JUL logger instance. */
    private Logger myJulLog = null;

    /** The Log under test. */
    private Log myTestLog = null;

    /**
     * Initializes the logger for the test.
     * 
     * @throws Exception
     *             On a failure initializing the logger.
     */
    @Before
    public void setUp() throws Exception {
        myCaptureHandler = new CaptureHandler();

        myJulLog = Logger.getLogger(Slf4jLogTest.class.getName());
        myJulLog.addHandler(myCaptureHandler);
        myJulLog.setUseParentHandlers(false);
        myJulLog.setLevel(Level.FINEST);

        LogFactory.reset();
        myTestLog = (Log) ourFactoryMethod.invoke(ourFactoryMethod
                .getDeclaringClass().newInstance(), Slf4jLogTest.class);
    }

    /**
     * Cleanup after the test.
     * 
     * @throws Exception
     *             On a failure cleaning up the logger.
     */
    @After
    public void tearDown() throws Exception {
        myJulLog.removeHandler(myCaptureHandler);
        myJulLog.setUseParentHandlers(true);
        myJulLog.setLevel(null);

        myCaptureHandler.close();

        myCaptureHandler = null;
        myJulLog = null;
        myTestLog = null;

        LogFactory.reset();
    }

    /**
     * Test for the {@link AbstractLog#debug(String)} method.
     */
    @Test
    public void testDebugString() {
        final String method = "testDebugString";
        final String messsage = "Debug Message";
        final Level level = Level.FINE;

        myTestLog.debug(messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#debug(String, Object...)} method.
     */
    @Test
    public void testDebugStringObjectArray() {
        final String method = "testDebugStringObjectArray";
        final String messsage = "Debug Message";
        final Level level = Level.FINE;

        myTestLog.debug(messsage, "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#debug(Throwable, String, Object...)}
     * method.
     */
    @Test
    public void testDebugThrowableStringObjectArray() {
        final String method = "testDebugThrowableStringObjectArray";
        final String messsage = "Debug Message";
        final Level level = Level.FINE;
        final Throwable thrown = new Throwable();

        myTestLog.debug(thrown, messsage, "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), sameInstance(thrown));
    }

    /**
     * Test for the {@link AbstractLog#error(String)} method.
     */
    @Test
    public void testErrorString() {
        final String method = "testErrorString";
        final String messsage = "Error Message";
        final Level level = Level.SEVERE;

        myTestLog.error(messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#error(String)} method.
     */
    @Test
    public void testErrorStringDisabled() {
        final String messsage = "Error Message";

        myJulLog.setLevel(Level.OFF);

        myTestLog.error(messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(0));
    }

    /**
     * Test for the {@link AbstractLog#error(String, Object...)} method.
     */
    @Test
    public void testErrorStringObjectArray() {
        final String method = "testErrorStringObjectArray";
        final String messsage = "{} - {} {}"; // "Error - Hello World";
        final Level level = Level.SEVERE;

        myTestLog.error("{} - {} {}", "Error", "Hello", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#error(String, Object...)} method.
     */
    @Test
    public void testErrorStringObjectArrayWithNullArray() {
        final String method = "testErrorStringObjectArrayWithNullArray";
        final String messsage = "Error - Hello World";
        final Level level = Level.SEVERE;

        myTestLog.error("Error - Hello World", (Object[]) null);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#error(Throwable, String, Object...)}
     * method.
     */
    @Test
    public void testErrorThrowableStringObjectArray() {
        final String method = "testErrorThrowableStringObjectArray";
        final String messsage = "Error - Hello {}"; // "Error - Hello World";
        final Level level = Level.SEVERE;
        final Throwable thrown = new Throwable();

        myTestLog.error(thrown, "Error - Hello {}", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), sameInstance(thrown));
    }

    /**
     * Test for the {@link LogFactory#getLog(Class)} method.
     */
    @Test
    public void testGetLog() {
        final Log log = LogFactory.getLog(getClass());

        assertThat(log, instanceOf(Slf4jLog.class));
    }

    /**
     * Test for the {@link AbstractLog#info(String)} method.
     */
    @Test
    public void testInfoString() {
        final String method = "testInfoString";
        final String messsage = "Info Message";
        final Level level = Level.INFO;

        myTestLog.info(messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#info(String, Object...)} method.
     */
    @Test
    public void testInfoStringObjectArray() {
        final String method = "testInfoStringObjectArray";
        final String messsage = "Info - Hello {}"; // "Info - Hello World";
        final Level level = Level.INFO;

        myTestLog.info("Info - Hello {}", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#info(Throwable, String, Object...)}
     * method.
     */
    @Test
    public void testInfoThrowableStringObjectArray() {
        final String method = "testInfoThrowableStringObjectArray";
        final String messsage = "Info - Hello {}"; // "Info - Hello World";
        final Level level = Level.INFO;
        final Throwable thrown = new Throwable();

        myTestLog.info(thrown, "Info - Hello {}", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), sameInstance(thrown));
    }

    /**
     * Test for the {@link AbstractLog#log(Level, String)} method.
     */
    @Test
    public void testLogLevelString() {
        final String method = "testLogLevelString";
        final String messsage = "Log Message";
        final Level level = Level.WARNING;

        myTestLog.log(level, messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#log(Level, String, Object...)} method.
     */
    @Test
    public void testLogLevelStringObjectArray() {
        final String method = "testLogLevelStringObjectArray";
        final String messsage = "{}{}{}"; // "Info - Hello World";
        final Level level = Level.INFO;

        myTestLog.info("{}{}{}", "Info - ", "Hello", " World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#log(Level, Throwable, String, Object...)}
     * method.
     */
    @Test
    public void testLogLevelThrowableStringObjectArray() {
        final String method = "testLogLevelThrowableStringObjectArray";
        final String messsage = "Debug - Hello {}"; // "Debug - Hello World";
        final Level level = Level.FINE;
        final Throwable thrown = new Throwable();

        myTestLog.debug(thrown, "Debug - Hello {}", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), sameInstance(thrown));
    }

    /**
     * Test for the {@link AbstractLog#warn(String)} method.
     */
    @Test
    public void testWarnString() {
        final String method = "testWarnString";
        final String messsage = "Warning Message";
        final Level level = Level.WARNING;

        myTestLog.warn(messsage);

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#warn(String, Object...)} method.
     */
    @Test
    public void testWarnStringObjectArray() {
        final String method = "testWarnStringObjectArray";
        final String messsage = "{} - Hello {}"; // "Warn - Hello World";
        final Level level = Level.WARNING;

        myTestLog.warn("{} - Hello {}", "Warn", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), nullValue());
    }

    /**
     * Test for the {@link AbstractLog#warn(Throwable, String, Object...)}
     * method.
     */
    @Test
    public void testWarnThrowableStringObjectArray() {
        final String method = "testWarnThrowableStringObjectArray";
        final String messsage = "Warn - Hello {}"; // "Warn - Hello World";
        final Level level = Level.WARNING;
        final Throwable thrown = new Throwable();

        myTestLog.warn(thrown, "Warn - Hello {}", "World");

        final List<LogRecord> records = myCaptureHandler.getRecords();
        assertThat(records.size(), is(1));

        final LogRecord record = records.get(0);
        assertThat(record.getLevel(), is(level));
        assertThat(record.getLoggerName(), is(Slf4jLogTest.class.getName()));
        assertThat(record.getMessage(), is(messsage));
        assertThat(record.getSourceClassName(),
                is(Slf4jLogTest.class.getName()));
        assertThat(record.getSourceMethodName(), is(method));
        assertThat(record.getThrown(), sameInstance(thrown));
    }
}
