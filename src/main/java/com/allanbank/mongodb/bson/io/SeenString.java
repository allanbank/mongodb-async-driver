/*
 * #%L
 * SeenString.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.bson.io;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * SeenString is a record of the byte[], value and count for each string/byte[]
 * the cache sees.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
class SeenString {
    /**
     * Atomic updated for all of the SeenString instances to cut down on the
     * number of {@link AtomicInteger} entries required.
     */
    private final static AtomicIntegerFieldUpdater<SeenString> ourCountUpdater = AtomicIntegerFieldUpdater
            .newUpdater(SeenString.class, "myCount");

    /** The encoded bytes for the seen string. */
    private final byte[] myBytes;

    /** The number of times the string is seen. */
    private volatile int myCount;

    /** The value for the seen string. */
    private final String myValue;

    /**
     * Creates a new SeenString.
     * 
     * @param source
     *            The source of the bytes in the string.
     * @param offset
     *            The offset of the first byte to decode.
     * @param length
     *            The length of the bytes to decode with a terminal zero byte.
     * @param decoded
     *            The value for the seen string.
     */
    public SeenString(final byte[] source, final int offset, final int length,
            final String decoded) {
        myBytes = Arrays.copyOfRange(source, offset, offset + length);
        myValue = decoded;
        myCount = 0;
    }

    /**
     * Returns the encoded bytes for the seen string.
     * 
     * @return The encoded bytes for the seen string.
     */
    public byte[] getBytes() {
        return myBytes;
    }

    /**
     * Returns the number of times the string is seen.
     * 
     * @return The number of times the string is seen.
     */
    public int getCount() {
        return myCount;
    }

    /**
     * Returns the decoded string for the seen string.
     * 
     * @return The decoded string for the seen string.
     */
    public String getValue() {
        return myValue;
    }

    /**
     * Increments the number of times that the string has been seen.
     */
    public void incrementCount() {
        ourCountUpdater.incrementAndGet(this);
    }

    /**
     * Sets the count to zero.
     * 
     * @return the previous value for the count.
     */
    public int reset() {
        return ourCountUpdater.getAndSet(this, 0);
    }
}