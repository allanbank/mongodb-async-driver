/*
 * #%L
 * AbstractLog.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.annotation.concurrent.ThreadSafe;

/**
 * AbstractLog implements all of the public log method to delegate to a single
 * {@link #doLog(Level, Throwable, String, Object...)} method.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public abstract class AbstractLog
        implements Log {

    /** The name of this class. */
    protected static final Object CLASS_NAME = AbstractLog.class.getName();

    /** The period between logging level checks. */
    private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

    /** The last time we checked the enabled logging level. */
    private long myLastLevelCheck;

    /** The level that is enabled when we last checked. */
    private Level myLevel;

    /**
     * Creates a new AbstractLog.
     */
    /* package */AbstractLog() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#debug(String)
     */
    @Override
    public final void debug(final String message) {
        doLog(Level.FINE, (Throwable) null, message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#debug(String, Object[])
     */
    @Override
    public final void debug(final String template, final Object... args) {
        doLog(Level.FINE, null, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#debug(Throwable, String, Object[])
     */
    @Override
    public final void debug(final Throwable thrown, final String template,
            final Object... args) {
        doLog(Level.FINE, thrown, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#error(String)
     */
    @Override
    public final void error(final String message) {
        doLog(Level.SEVERE, (Throwable) null, message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#error(String, Object[])
     */
    @Override
    public final void error(final String template, final Object... args) {
        doLog(Level.SEVERE, null, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#error(Throwable, String, Object[])
     */
    @Override
    public final void error(final Throwable thrown, final String template,
            final Object... args) {
        doLog(Level.SEVERE, thrown, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#info(String)
     */
    @Override
    public final void info(final String message) {
        doLog(Level.INFO, (Throwable) null, message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#info(String, Object[])
     */
    @Override
    public final void info(final String template, final Object... args) {
        doLog(Level.INFO, null, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#info(Throwable, String, Object[])
     */
    @Override
    public final void info(final Throwable thrown, final String template,
            final Object... args) {
        doLog(Level.INFO, thrown, template, args);
    }

    /**
     * Returns true if logging {@link Level#FINE DEBUG} messages is enabled,
     * false otherwise.
     *
     * @return True if logging debug messages is enabled.
     */
    @Override
    public final boolean isDebugEnabled() {
        return level().intValue() <= Level.FINE.intValue();
    }

    /**
     * Returns true if logging messages with the specified level is enabled,
     * false otherwise.
     *
     * @param level
     *            The level to determine if it is enabled.
     * @return True if logging with the specified level is enabled.
     */
    @Override
    public final boolean isEnabled(final Level level) {
        return level().intValue() <= level.intValue();
    }

    /**
     * Returns true if logging {@link Level#SEVERE ERROR} messages is enabled,
     * false otherwise.
     *
     * @return True if logging {@link Level#SEVERE ERROR} messages is enabled.
     */
    @Override
    public final boolean isErrorEnabled() {
        return level().intValue() <= Level.SEVERE.intValue();
    }

    /**
     * Returns true if logging {@link Level#INFO} messages is enabled, false
     * otherwise.
     *
     * @return True if logging {@link Level#INFO} messages is enabled.
     */
    @Override
    public final boolean isInfoEnabled() {
        return level().intValue() <= Level.INFO.intValue();
    }

    /**
     * Returns true if logging {@link Level#WARNING} messages is enabled, false
     * otherwise.
     *
     * @return True if logging {@link Level#WARNING} messages is enabled.
     */
    @Override
    public final boolean isWarnEnabled() {
        return level().intValue() <= Level.WARNING.intValue();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#log(Level, String)
     */
    @Override
    public final void log(final Level level, final String message) {
        doLog(level, (Throwable) null, message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#log(Level, String, Object[])
     */
    @Override
    public final void log(final Level level, final String template,
            final Object... args) {
        doLog(level, null, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#log(Level, Throwable, String, Object[])
     */
    @Override
    public final void log(final Level level, final Throwable thrown,
            final String template, final Object... args) {
        doLog(level, thrown, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to forces the next log attempt to re-determine the logging
     * level.
     * </p>
     *
     * @see Log#warn(String)
     */
    @Override
    public void reset() {
        myLastLevelCheck = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#warn(String)
     */
    @Override
    public final void warn(final String message) {
        doLog(Level.WARNING, (Throwable) null, message);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#warn(String, Object[])
     */
    @Override
    public final void warn(final String template, final Object... args) {
        doLog(Level.WARNING, null, template, args);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to call {@link #doLog(Level, Throwable, String, Object...)}.
     * </p>
     *
     * @see Log#warn(Throwable, String, Object[])
     */
    @Override
    public final void warn(final Throwable thrown, final String template,
            final Object... args) {
        doLog(Level.WARNING, thrown, template, args);
    }

    /**
     * Delegate method for the {@link Log} implementation.
     *
     * @return The current log level.
     */
    protected abstract Level doGetLevel();

    /**
     * Delegate method for the {@link Log} implementation.
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
    protected abstract void doLog(Level level, Throwable thrown,
            String template, Object... args);

    /**
     * Returns the current logging level.
     *
     * @return The current logging level.
     */
    protected final Level level() {
        final long now = System.currentTimeMillis();
        if ((myLastLevelCheck + ONE_MINUTE) < now) {
            myLastLevelCheck = now;
            myLevel = doGetLevel();
        }

        return myLevel;
    }

}