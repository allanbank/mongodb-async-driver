/*
 * #%L
 * EndianUtils.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * Utilities to deal with integer endian differences.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class EndianUtils {
    /**
     * Performs a byte-order swap for a 32-bit signed integer.
     * 
     * @param value
     *            The value to swap.
     * @return The swapped value.
     */
    public static int swap(final int value) {
        return (((value << 24) & 0xFF000000) | ((value << 8) & 0x00FF0000)
                | ((value >> 8) & 0x0000FF00) | ((value >> 24) & 0x000000FF));
    }

    /**
     * Performs a byte-order swap for a 64-bit signed integer.
     * 
     * @param value
     *            The value to swap.
     * @return The swapped value.
     */
    public static long swap(final long value) {
        return (((value << 56) & 0xFF00000000000000L)
                | ((value << 40) & 0x00FF000000000000L)
                | ((value << 24) & 0x0000FF0000000000L)
                | ((value << 8) & 0x000000FF00000000L)
                | ((value >> 8) & 0x00000000FF000000L)
                | ((value >> 24) & 0x0000000000FF0000L)
                | ((value >> 40) & 0x000000000000FF00L) | ((value >> 56) & 0x00000000000000FFL));
    }

}
