/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

import java.util.logging.Level;

/**
 * Log is the simplified logging interface used by the driver. It will
 * dynamically wrap either SLF4J (if available) or Java Util Logging (as a
 * fall-back).
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Log {
    /**
     * The token to replace with the arguments to the log message. Unlike SLF4J
     * we do support escaping of this token.
     */
    public static final String REPLACE_TOKEN = "{}";

    /**
     * Logs a message at the {@link Level#FINE DEBUG} level.
     * 
     * @param message
     *            The message to log.
     */
    public void debug(String message);

    /**
     * Logs a message at the DEBUG level.
     * 
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void debug(String template, Object... args);

    /**
     * Logs a message at the DEBUG level.
     * 
     * @param thrown
     *            The exception associated with the log message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void debug(Throwable thrown, String template, Object... args);

    /**
     * Logs a message at the {@link Level#SEVERE ERROR} level.
     * 
     * @param message
     *            The message to log.
     */
    public void error(String message);

    /**
     * Logs a message at the ERROR level.
     * 
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void error(String template, Object... args);

    /**
     * Logs a message at the ERROR level.
     * 
     * @param thrown
     *            The exception associated with the log message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void error(Throwable thrown, String template, Object... args);

    /**
     * Logs a message at the {@link Level#INFO} level.
     * 
     * @param message
     *            The message to log.
     */
    public void info(String message);

    /**
     * Logs a message at the INFO level.
     * 
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void info(String template, Object... args);

    /**
     * Logs a message at the INFO level.
     * 
     * @param thrown
     *            The exception associated with the log message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void info(Throwable thrown, String template, Object... args);

    /**
     * Logs a message at the specified level.
     * 
     * @param level
     *            The logging level for the message.
     * @param message
     *            The message to log.
     */
    public void log(Level level, String message);

    /**
     * Logs a message at the specified level.
     * 
     * @param level
     *            The logging level for the message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void log(Level level, String template, Object... args);

    /**
     * Logs a message at the specified level.
     * 
     * @param level
     *            The logging level for the message.
     * @param thrown
     *            The exception associated with the log message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void log(Level level, Throwable thrown, String template,
            Object... args);

    /**
     * Logs a message at the {@link Level#WARNING} level.
     * 
     * @param message
     *            The message to log.
     */
    public void warn(String message);

    /**
     * Logs a message at the WARNING level.
     * 
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void warn(String template, Object... args);

    /**
     * Logs a message at the WARNING level.
     * 
     * @param thrown
     *            The exception associated with the log message.
     * @param template
     *            The message template to log.
     * @param args
     *            The arguments to replace the <code>{}</code> entries in the
     *            template.
     */
    public void warn(Throwable thrown, String template, Object... args);

}
