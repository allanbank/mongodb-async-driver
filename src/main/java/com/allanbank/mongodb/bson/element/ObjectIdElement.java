/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;

/**
 * A wrapper for a BSON Object Id.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ObjectIdElement extends AbstractElement {

	/** The counter to add to the machine id. */
	private static final AtomicLong COUNTER = new AtomicLong(0);

	/** The BSON type for a Object Id. */
	public static final String DEFAULT_NAME = "_id";

	/** The current process's machine id. */
	public static final long MACHINE_ID;

	/** The BSON type for a Object Id. */
	public static final ElementType TYPE = ElementType.OBJECT_ID;

	static {
		long value = 0;
		SecureRandom rand = new SecureRandom();
		try {
			boolean foundIface = true;
			MessageDigest md5 = MessageDigest.getInstance("MD5");

			try {
				Enumeration<NetworkInterface> ifaces = NetworkInterface
						.getNetworkInterfaces();
				while (ifaces.hasMoreElements()) {
					try {
						NetworkInterface iface = ifaces.nextElement();

						if (!iface.isLoopback()) {
							md5.update(iface.getHardwareAddress());
							foundIface = true;
						}
					} catch (Throwable tryAnotherIface) {
						// Noting to do. Try the next one.
					}
				}
			} catch (Throwable tryTheHostName) {
				// Nothing to do here. Fall through.
			}

			if (!foundIface) {
				md5.update(InetAddress.getLocalHost().getHostName()
						.getBytes("UTF8"));
			}

			byte[] hash = md5.digest();
			value += (hash[0] & 0xFF);
			value <<= Byte.SIZE;
			value += (hash[1] & 0xFF);
			value <<= Byte.SIZE;
			value += (hash[2] & 0xFF);
			value <<= Byte.SIZE;
		} catch (Throwable t) {
			// Degenerate to a random machine id.
			for (int i = 0; i < 3; ++i) {
				value += rand.nextInt(256);
				value <<= Byte.SIZE;
			}
		}

		// Use a random value for the pid.
		value += rand.nextInt(256);
		value <<= Byte.SIZE;
		value += rand.nextInt(256);

		MACHINE_ID = (value << 24);
	}

	/**
	 * Generates the current timestamp value. This is the number of
	 * <b>seconds</b> since the Unix Epoch.
	 * 
	 * @return The unique object id value.
	 */
	private static int now() {
		return (int) TimeUnit.MILLISECONDS
				.toSeconds(System.currentTimeMillis());
	}

	/**
	 * Generates the current timestamp value. This is the number of
	 * <b>seconds</b> since the Unix Epoch.
	 * 
	 * @return The unique object id value.
	 */
	private static long processId() {
		return MACHINE_ID + (COUNTER.incrementAndGet() & 0xFFFFFFL);
	}

	/** The BSON Object Id's machine identifier. */
	private final long myMachineId;

	/** The BSON ObjectId's timestamp. */
	private final int myTimestamp;

	/**
	 * Constructs a new {@link ObjectIdElement}.
	 */
	public ObjectIdElement() {
		this(DEFAULT_NAME, now(), processId());
	}

	/**
	 * Constructs a new {@link ObjectIdElement}.
	 * 
	 * @param name
	 *            The name for the BSON Object Id.
	 * @param timestamp
	 *            The BSON Object Id timestamp.
	 * @param machineId
	 *            The BSON Object Id machine id.
	 */
	public ObjectIdElement(String name, int timestamp, long machineId) {
		this(TYPE, name, timestamp, machineId);
	}

	/**
	 * Constructs a new {@link ObjectIdElement}.
	 * 
	 * @param type
	 *            The type of the inherited element.
	 * @param name
	 *            The name for the BSON Object Id.
	 * @param timestamp
	 *            The BSON Object Id timestamp.
	 * @param machineId
	 *            The BSON Object Id machine id.
	 */
	protected ObjectIdElement(ElementType type, String name, int timestamp,
			long machineId) {
		super(type, name);

		myTimestamp = timestamp;
		myMachineId = machineId;
	}

	/**
	 * The lower 8 bytes of the object id.
	 * 
	 * @return The lower 8 bytes of the object id.
	 */
	public long getMachineId() {
		return myMachineId;
	}

	/**
	 * The upper 4 bytes of the object id.
	 * 
	 * @return The upper 4 bytes of the object id.
	 */
	public int getTimestamp() {
		return myTimestamp;
	}

	/**
	 * Accepts the visitor and calls the {@link Visitor#visitObjectId} method.
	 * 
	 * @see Element#accept(Visitor)
	 */
	@Override
	public void accept(Visitor visitor) {
		visitor.visitObjectId(getName(), getTimestamp(), getMachineId());
	}

	/**
	 * Computes a reasonable hash code.
	 * 
	 * @return The hash code value.
	 */
	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + super.hashCode();
		result = 31 * result + (int) (myMachineId & 0xFFFFFFFF);
		result = 31 * result + (int) ((myMachineId >> 32) & 0xFFFFFFFF);
		result = 31 * result + myTimestamp;
		return result;
	}

	/**
	 * Determines if the passed object is of this same type as this object and
	 * if so that its fields are equal.
	 * 
	 * @param object
	 *            The object to compare to.
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object) {
		boolean result = false;
		if (this == object) {
			result = true;
		} else if ((object != null) && (getClass() == object.getClass())) {
			ObjectIdElement other = (ObjectIdElement) object;

			result = (myMachineId == other.myMachineId)
					&& (myTimestamp == other.myTimestamp)
					&& super.equals(object);
		}
		return result;
	}

	/**
	 * String form of the object.
	 * 
	 * @return A human readable form of the object.
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append('"');
		builder.append(getName());
		builder.append("\" : ObjectId(");

		String hex = Integer.toHexString(myTimestamp);
		builder.append("00000000".substring(hex.length()));
		builder.append(hex);

		hex = Long.toHexString(myMachineId);
		builder.append("0000000000000000".substring(hex.length()));
		builder.append(hex);

		builder.append(")");

		return builder.toString();
	}
}
