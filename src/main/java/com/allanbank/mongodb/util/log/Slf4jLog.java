/*
 * #%L
 * Slf4jLog.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Slf4jLog is the simplified logging implementation for SLF4J logging facade.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Slf4jLog extends AbstractLog {

    /** The delegate for the log to a SLF4J <code>LocationAwareLogger</code>. */
    private final Object myDelegate;

    /**
     * The <code>log(Marker, String, int, String, Object[])</code> method from
     * the <code>LocationAwareLogger</code> interface.
     */
    private final Method myLogMethod;

    /**
     * Creates a new {@link Slf4jLog}.
     * 
     * @param logMethod
     *            The <code>log(Marker, String, int, String, Object[])</code>
     *            method from the <code>LocationAwareLogger</code> interface.
     * @param logger
     *            The SLF4J Logger.
     */
    protected Slf4jLog(final Method logMethod, final Object logger) {
        myDelegate = logger;
        myLogMethod = logMethod;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link LogRecord} based on the log information.
     * </p>
     * 
     * @see AbstractLog#doLog(Level, Throwable, String, Object...)
     */
    @Override
    protected final void doLog(final Level level, final Throwable thrown,
            final String template, final Object... args) {
        // Note the name of the class is the AbstractLog which is where all of
        // the public log methods are implemented.
        try {
            myLogMethod.invoke(myDelegate, new Object[] { null, CLASS_NAME,
                    toInt(level), template, args, thrown });
        }
        catch (final IllegalArgumentException e) {
            Logger.getLogger(Slf4jLog.class.getName()).log(Level.WARNING,
                    "Failed to log a message: " + e.getMessage(), e);
        }
        catch (final IllegalAccessException e) {
            Logger.getLogger(Slf4jLog.class.getName()).log(Level.WARNING,
                    "Failed to log a message: " + e.getMessage(), e);
        }
        catch (final InvocationTargetException e) {
            Logger.getLogger(Slf4jLog.class.getName()).log(Level.WARNING,
                    "Failed to log a message: " + e.getMessage(), e);
        }
    }

    /**
     * Returns the integer level for the {@link Level} as defined in the
     * LocationAwareLogger interface.
     * 
     * @param level
     *            The level to convert.
     * @return The integer level.
     */
    private Integer toInt(final Level level) {
        final int levelIntValue = (level != null) ? level.intValue() : 0;

        // Note: Return values are from the <code>LocationAwareLogger</code>
        // interface.
        if (levelIntValue == 0) {
            return 0;
        }
        else if (levelIntValue <= Level.FINE.intValue()) {
            return 10;
        }
        else if (levelIntValue <= Level.INFO.intValue()) {
            return 20;
        }
        else if (levelIntValue <= Level.WARNING.intValue()) {
            return 30;
        }
        else {
            return 40;
        }
    }
}
