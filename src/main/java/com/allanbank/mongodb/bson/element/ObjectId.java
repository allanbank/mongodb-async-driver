/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson.element;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.allanbank.mongodb.bson.io.EndianUtils;
import com.allanbank.mongodb.util.IOUtils;

/**
 * An Object Id.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ObjectId implements Serializable {

    /** The current process's machine id. */
    public static final long MACHINE_ID;

    /** The counter to add to the machine id. */
    private static final AtomicLong COUNTER;

    /** Serialization version for the class. */
    private static final long serialVersionUID = -3035334151717895487L;

    static {
        long value = 0;
        final SecureRandom rand = new SecureRandom();
        try {
            boolean foundIface = true;
            final MessageDigest md5 = MessageDigest.getInstance("MD5");

            try {
                final Enumeration<NetworkInterface> ifaces = NetworkInterface
                        .getNetworkInterfaces();
                while (ifaces.hasMoreElements()) {
                    try {
                        final NetworkInterface iface = ifaces.nextElement();

                        if (!iface.isLoopback()) {
                            md5.update(iface.getHardwareAddress());
                            foundIface = true;
                        }
                    }
                    catch (final Throwable tryAnotherIface) {
                        // Noting to do. Try the next one.
                        tryAnotherIface.hashCode(); // PMD - Shhhh.
                    }
                }
            }
            catch (final Throwable tryTheHostName) {
                // Nothing to do here. Fall through.
                tryTheHostName.hashCode(); // PMD - Shhhh.
            }

            if (!foundIface) {
                md5.update(InetAddress.getLocalHost().getHostName()
                        .getBytes("UTF8"));
            }

            final byte[] hash = md5.digest();
            value += (hash[0] & 0xFF);
            value <<= Byte.SIZE;
            value += (hash[1] & 0xFF);
            value <<= Byte.SIZE;
            value += (hash[2] & 0xFF);
            value <<= Byte.SIZE;
        }
        catch (final Throwable t) {
            // Degenerate to a random machine id.
            for (int i = 0; i < 3; ++i) {
                value += rand.nextInt(256);
                value <<= Byte.SIZE;
            }
        }

        // Try and find the process id from the runtime.
        int processId;
        try {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            String processName = runtime.getName();
            int atLoc = processName.indexOf('@');
            if (atLoc >= 0) {
                String pidString = processName.substring(0, atLoc);
                processId = Integer.parseInt(pidString);
            }
            else {
                // Degenerate to a random process id.
                processId = rand.nextInt();
            }

        }
        catch (Throwable t) {
            // Degenerate to a random process id.
            processId = rand.nextInt();
        }

        value += ((processId >> Byte.SIZE) & 0xFF);
        value <<= Byte.SIZE;
        value += (processId & 0xFF);

        MACHINE_ID = (value << 24);
        COUNTER = new AtomicLong(rand.nextLong() & 0xFFFFFFL);
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
     * Constructs a new {@link ObjectId}.
     */
    public ObjectId() {
        this(now(), processId());
    }

    /**
     * Constructs a new {@link ObjectId}.
     * 
     * @param timestamp
     *            The BSON Object Id timestamp.
     * @param machineId
     *            The BSON Object Id machine id.
     */
    public ObjectId(final int timestamp, final long machineId) {
        myTimestamp = timestamp;
        myMachineId = machineId;
    }

    /**
     * Constructs a new {@link ObjectId}.
     * 
     * @param hexBytes
     *            The hex encoded byte value.
     * @throws IllegalArgumentException
     *             If the hex encoded string is not 24 characters.
     */
    public ObjectId(final String hexBytes) throws IllegalArgumentException {

        if (hexBytes.length() != 24) {
            throw new IllegalArgumentException(
                    "Invalid ObjectId value.  Must be a 24 character hex string.");
        }

        final byte[] bytes = IOUtils.hexToBytes(hexBytes);
        int timestamp = 0;
        for (int i = 0; i < 4; ++i) {
            int value = (bytes[i] & 0xFF);
            value <<= (Byte.SIZE * i);
            timestamp += value;
        }

        long machineId = 0;
        for (int i = 4; i < 12; ++i) {
            long value = (bytes[i] & 0xFF);
            value <<= (Byte.SIZE * (i - 4));
            machineId += value;
        }

        myTimestamp = EndianUtils.swap(timestamp);
        myMachineId = EndianUtils.swap(machineId);
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
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final ObjectId other = (ObjectId) object;

            result = (myMachineId == other.myMachineId)
                    && (myTimestamp == other.myTimestamp);
        }
        return result;
    }

    /**
     * The lower 8 bytes of the object id. This is the machine identifier field
     * and counter.
     * 
     * @return The lower 8 bytes of the object id.
     */
    public long getMachineId() {
        return myMachineId;
    }

    /**
     * The upper 4 bytes of the object id. This is the <b>seconds</b> since the
     * UNIX Epoch.
     * 
     * @return The upper 4 bytes of the object id.
     */
    public int getTimestamp() {
        return myTimestamp;
    }

    /**
     * The low 3 byte value of the machine id.
     * 
     * @return The low 3 byte value of the machine id.
     */
    public int getCounterField() {
        return (int) (myMachineId & 0xFFFFFFL);
    }

    /**
     * Middle 2 byte process id field from the machine id.
     * 
     * @return The lower 8 bytes of the object id.
     */
    public int getPidField() {
        return (int) ((myMachineId >> 24) & 0xFFFFL);
    }

    /**
     * The upper 3 bytes in the machine id.
     * 
     * @return The upper 3 bytes of the object id.
     */
    public int getMachineIdentifier() {
        return (int) ((myMachineId >> 40) & 0xFFFFFFL);
    }

    /**
     * Computes a reasonable hash code.
     * 
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 0;
        result += (int) ((myMachineId >> 32) & 0xFFFFFFFF);
        result += (int) (myMachineId & 0xFFFFFFFF);
        result += myTimestamp;
        return result;
    }

    /**
     * Returns the HEX string form of the ObjectId.
     * 
     * @return The HEX string form of the ObjectId.
     */
    public String toHexString() {
        final StringBuilder builder = new StringBuilder();
        String hex = Integer.toHexString(myTimestamp);
        builder.append("00000000".substring(hex.length()));
        builder.append(hex);

        hex = Long.toHexString(myMachineId);
        builder.append("0000000000000000".substring(hex.length()));
        builder.append(hex);
        return builder.toString();
    }

    /**
     * String form of the object.
     * 
     * @return A human readable form of the object.
     * 
     * @see Object#toString()
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("ObjectId(");
        builder.append(toHexString());
        builder.append(")");

        return builder.toString();
    }
}
