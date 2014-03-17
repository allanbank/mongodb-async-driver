/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util.log;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Slf4jLogFactory provides factory to create {@link Slf4jLog} instances.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Slf4jLogFactory extends LogFactory {
	/**
	 * The <code>log(Marker, String, int, String, Object[])</code> method from
	 * the <code>LocationAwareLogger</code> interface.
	 */
	private final Method myLogMethod;

	/**
	 * The <code>getLogger(Class)</code> method from the
	 * <code>LoggerFactory</code> interface.
	 */
	private final Method myLogFactoryMethod;

	/** The <code>LocationAwareLogger</code> class. */
	private final Class<?> myLocationAwareLoggerClass;

	public Slf4jLogFactory() throws RuntimeException {
		try {
			Class<?> logFactoryClass = Class.forName("org.slf4j.LoggerFactory");
			myLogFactoryMethod = logFactoryClass.getMethod("getLogger",
					Class.class);

			myLocationAwareLoggerClass = Class
					.forName("org.slf4j.spi.LocationAwareLogger");
			Class<?> markerClass = Class.forName("org.slf4j.Marker");
			myLogMethod = myLocationAwareLoggerClass.getMethod("log",
					markerClass, String.class, int.class, String.class,
					Object[].class, Throwable.class);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to return a {@link Slf4jLog} instance if SLF4J LoggerFactory
	 * returns a LocationAwareLogger instance. Otherwise a {@link JulLog} is
	 * returned.
	 * </p>
	 */
	protected Log doGetLog(Class<?> clazz) {
		Log log = null;
		try {
			Object logger = myLogFactoryMethod.invoke(null, clazz);
			if (myLocationAwareLoggerClass.isInstance(logger)) {
				log = new Slf4jLog(myLogMethod, logger);
			}
		} catch (IllegalAccessException e) {
			// Fall through.
		} catch (InvocationTargetException e) {
			// Fall through.
		} catch (RuntimeException e) {
			// Fall through.
		}

		// Fall back to JUL logging.
		if (log == null) {
			log = new JulLog(clazz);
		}

		return log;
	}
}
