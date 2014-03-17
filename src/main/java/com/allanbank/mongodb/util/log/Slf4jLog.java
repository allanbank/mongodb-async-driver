/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Slf4jLog is the simplified logging implementation for SLF4J logging facade.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Slf4jLog extends AbstractLog implements Log {

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
	 * @param formatMethod
	 *            The
	 *            <code>MessageFormatter.arrayFormat(String, Object[])</code>
	 *            method.
	 * @param logger
	 *            The SLF4J Logger.
	 */
	protected Slf4jLog(Method logMethod, Object logger) {
		myDelegate = logger;
		myLogMethod = logMethod;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to create a {@link LogRecord} based on the log information.
	 * </p>
	 * 
	 * @see Log#doLog(Level, Throwable, String, Object[])
	 */
	@Override
	protected final void doLog(Level level, Throwable thrown, String template,
			Object... args) {
		// Note the name of the class is the AbstractLog which is where all of
		// the public log methods are implemented.
		try {
			myLogMethod.invoke(myDelegate, new Object[] { null, CLASS_NAME,
					toInt(level), template, args, thrown });
		} catch (IllegalArgumentException e) {
			// TODO: What now?
		} catch (IllegalAccessException e) {
			// TODO: What now?
		} catch (InvocationTargetException e) {
			// TODO: What now?
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
	private Integer toInt(Level level) {
		int levelIntValue = (level != null) ? level.intValue() : 0;

		// Note: Return values are from the <code>LocationAwareLogger</code>
		// interface.
		if (levelIntValue == 0) {
			return 0;
		} else if (levelIntValue <= Level.FINE.intValue()) {
			return 10;
		} else if (levelIntValue <= Level.INFO.intValue()) {
			return 20;
		} else if (levelIntValue <= Level.WARNING.intValue()) {
			return 30;
		} else {
			return 40;
		}
	}
}
