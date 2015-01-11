/*
 * #%L
 * Log.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.util.log;

import java.util.logging.Level;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Log is the simplified logging interface used by the driver. It will
 * dynamically wrap either SLF4J (if available) or Java Util Logging (as a
 * fall-back).
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
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
     * Returns true if logging {@link Level#FINE DEBUG} messages is enabled,
     * false otherwise.
     *
     * @return True if logging debug messages is enabled.
     */
    public boolean isDebugEnabled();

    /**
     * Returns true if logging messages with the specified level is enabled,
     * false otherwise.
     *
     * @param level
     *            The level to determine if it is enabled.
     * @return True if logging with the specified level is enabled.
     */
    public boolean isEnabled(Level level);

    /**
     * Returns true if logging {@link Level#SEVERE ERROR} messages is enabled,
     * false otherwise.
     *
     * @return True if logging {@link Level#SEVERE ERROR} messages is enabled.
     */
    public boolean isErrorEnabled();

    /**
     * Returns true if logging {@link Level#INFO} messages is enabled, false
     * otherwise.
     *
     * @return True if logging {@link Level#INFO} messages is enabled.
     */
    public boolean isInfoEnabled();

    /**
     * Returns true if logging {@link Level#WARNING} messages is enabled, false
     * otherwise.
     *
     * @return True if logging {@link Level#WARNING} messages is enabled.
     */
    public boolean isWarnEnabled();

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
     * Resets the state of the logger.
     */
    public void reset();

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
