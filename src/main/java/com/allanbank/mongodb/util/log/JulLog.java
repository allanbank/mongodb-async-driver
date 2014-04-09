/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * JulLog is the simplified logging implementation for Java Util Logging.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JulLog extends AbstractLog {

    /** The delegate for the log to a {@link Logger}. */
    private final Logger myDelegate;

    /**
     * Creates a new {@link JulLog}.
     * 
     * @param clazz
     *            The class for the logger.
     */
    protected JulLog(final Class<?> clazz) {
        myDelegate = Logger.getLogger(clazz.getName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link LogRecord} based on the log information.
     * </p>
     * 
     * @see Log#log(Level, Throwable, String, Object[])
     */
    @Override
    protected final void doLog(final Level level, final Throwable thrown,
            final String template, final Object... args) {
        if (myDelegate.isLoggable(level)) {

            final Thread currentThread = Thread.currentThread();
            final LogRecord record = new LogRecord(level,
                    format(template, args));
            record.setLoggerName(myDelegate.getName());
            record.setThrown(thrown);
            record.setThreadID((int) Thread.currentThread().getId());

            // Note the name of the class is the AbstractLog which is where all
            // of
            // the public log methods are implemented.
            boolean lookingForThisClass = true;
            for (final StackTraceElement element : currentThread
                    .getStackTrace()) {
                final String className = element.getClassName();

                // Find this method (and maybe others from this class).
                if (lookingForThisClass) {
                    lookingForThisClass = !CLASS_NAME.equals(className);
                }
                else {
                    // And back until we are past this class.
                    if (!CLASS_NAME.equals(className)) {
                        record.setSourceClassName(className);
                        record.setSourceMethodName(element.getMethodName());
                        break;
                    }
                }
            }

            // Finally - log it.
            myDelegate.log(record);
        }
    }

    /**
     * Formats the message to be logged.
     * 
     * @param template
     *            The template for the message.
     * @param args
     *            The arguments to use to replace the {@value #REPLACE_TOKEN}
     *            entries in the template.
     * @return The formatted message.
     */
    private String format(final String template, final Object[] args) {
        String result = template;
        if ((args != null) && (args.length > 0)
                && (template.indexOf(REPLACE_TOKEN) >= 0)) {

            final StringBuilder builder = new StringBuilder(template.length());
            for (final Object arg : args) {
                final int index = result.indexOf(REPLACE_TOKEN);
                if (index < 0) {
                    break;
                }

                builder.append(result.substring(0, index));
                builder.append(arg);
                result = result.substring(index + 2);
            }

            builder.append(result);
            result = builder.toString();
        }
        return result;
    }

}
