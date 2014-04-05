/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * IOUtils provides helper methods for dealing with I/O operations.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class IOUtils {

    /** Base64 encoding array according to RFC 2045. */
    private static final char[] BASE_64_CHARS = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            + "abcdefghijklmnopqrstuvwxyz0123456789+/").toCharArray();

    /**
     * The mapping from a character (ascii) value to the corresponding Base64
     * (RFC-2045) 6-bit value.
     */
    private static final byte CHAR_TO_BASE_64_BITS[] = { -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59,
            60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1,
            -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
            38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51 };

    /** The mapping from a character (ascii) value to a nibble hex encoded. */
    private static final byte[] CHAR_TO_HEX_NIBBLE;

    /** Hex encoding characters. */
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /** The logger for the {@link IOUtils}. */
    private static final Log LOG = LogFactory.getLog(IOUtils.class);

    static {
        CHAR_TO_HEX_NIBBLE = new byte[128];
        Arrays.fill(CHAR_TO_HEX_NIBBLE, (byte) -1);
        CHAR_TO_HEX_NIBBLE['0'] = 0;
        CHAR_TO_HEX_NIBBLE['1'] = 1;
        CHAR_TO_HEX_NIBBLE['2'] = 2;
        CHAR_TO_HEX_NIBBLE['3'] = 3;
        CHAR_TO_HEX_NIBBLE['4'] = 4;
        CHAR_TO_HEX_NIBBLE['5'] = 5;
        CHAR_TO_HEX_NIBBLE['6'] = 6;
        CHAR_TO_HEX_NIBBLE['7'] = 7;
        CHAR_TO_HEX_NIBBLE['8'] = 8;
        CHAR_TO_HEX_NIBBLE['9'] = 9;
        CHAR_TO_HEX_NIBBLE['a'] = 0xa;
        CHAR_TO_HEX_NIBBLE['b'] = 0xb;
        CHAR_TO_HEX_NIBBLE['c'] = 0xc;
        CHAR_TO_HEX_NIBBLE['d'] = 0xd;
        CHAR_TO_HEX_NIBBLE['e'] = 0xe;
        CHAR_TO_HEX_NIBBLE['f'] = 0xf;
        CHAR_TO_HEX_NIBBLE['A'] = 0xA;
        CHAR_TO_HEX_NIBBLE['B'] = 0xB;
        CHAR_TO_HEX_NIBBLE['C'] = 0xC;
        CHAR_TO_HEX_NIBBLE['D'] = 0xD;
        CHAR_TO_HEX_NIBBLE['E'] = 0xE;
        CHAR_TO_HEX_NIBBLE['F'] = 0xF;
    }

    /**
     * Converts the Base64 (RFC 2045) String into a byte array.
     *
     * @param base64
     *            The Base64 string to convert.
     * @return The byte[] version.
     */
    public static byte[] base64ToBytes(final String base64) {
        final int base64Length = base64.length();
        final int numGroups = base64Length / 4;
        if ((4 * numGroups) != base64Length) {
            throw new IllegalArgumentException(
                    "String length must be a multiple of four.");
        }

        int missingBytesInLastGroup = 0;
        int numFullGroups = numGroups;
        if (base64Length != 0) {
            if (base64.charAt(base64Length - 1) == '=') {
                missingBytesInLastGroup++;
                numFullGroups--;
            }
            if (base64.charAt(base64Length - 2) == '=') {
                missingBytesInLastGroup++;
            }
        }
        final byte[] result = new byte[(3 * numGroups)
                - missingBytesInLastGroup];

        // Translate all 4 character groups from base64 to byte array elements
        int base64Index = 0;
        int resultIndex = 0;
        for (int i = 0; i < numFullGroups; i++) {
            final int b1 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            final int b2 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            final int b3 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            final int b4 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            result[resultIndex++] = (byte) ((b1 << 2) + (b2 >> 4));
            result[resultIndex++] = (byte) ((b2 << 4) + (b3 >> 2));
            result[resultIndex++] = (byte) ((b3 << 6) + b4);
        }

        // Translate partial group, if present
        if (missingBytesInLastGroup != 0) {
            final int b1 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            final int b2 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                    base64.charAt(base64Index++));
            result[resultIndex++] = (byte) ((b1 << 2) + (b2 >> 4));

            if (missingBytesInLastGroup == 1) {
                final int b3 = alphabetToBits(CHAR_TO_BASE_64_BITS,
                        base64.charAt(base64Index++));
                result[resultIndex++] = (byte) ((b2 << 4) + (b3 >> 2));
            }
        }

        return result;
    }

    /**
     * Closes the {@link Closeable} and logs any error.
     *
     * @param closeable
     *            The connection to close.
     */
    public static void close(final Closeable closeable) {
        if (closeable != null) {
            close(closeable, Level.FINE, "I/O Exception closing: "
                    + closeable.getClass().getSimpleName());
        }
    }

    /**
     * Closes the {@link Closeable} and logs any error.
     *
     * @param closeable
     *            The connection to close.
     * @param level
     *            The level to log on a failure.
     * @param message
     *            The message to log on a failure.
     */
    public static void close(final Closeable closeable, final Level level,
            final String message) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (final IOException ignored) {
                LOG.log(level, message);
            }
        }
    }

    /**
     * Converts the hex string to bytes.
     *
     * @param hex
     *            The HEX string to convert.
     * @return The byte[] version.
     */
    public static byte[] hexToBytes(final String hex) {
        final String trimmed = hex.trim();

        final int length = trimmed.length();
        if ((length & 1) == 1) {
            throw new IllegalArgumentException(
                    "A hex string must be an even number of characters: '"
                            + trimmed + "'");
        }

        final byte[] bytes = new byte[length >> 1];
        for (int i = 0; i < length; i += 2) {

            final int v1 = alphabetToBits(CHAR_TO_HEX_NIBBLE, trimmed.charAt(i));
            final int v2 = alphabetToBits(CHAR_TO_HEX_NIBBLE,
                    trimmed.charAt(i + 1));

            bytes[i >> 1] = (byte) (((v1 << 4) & 0xF0) + (v2 & 0x0F));
        }

        return bytes;
    }

    /**
     * Converts the byte array into a Base64 (RFC 2045) string.
     *
     * @param bytes
     *            The bytes to convert.
     * @return The string version.
     */
    public static String toBase64(final byte[] bytes) {
        final int length = bytes.length;

        // Create a buffer with the maximum possible length.
        final StringBuffer result = new StringBuffer(4 * ((length + 2) / 3));

        // Handle each 3 byte group.
        int index = 0;
        final int numGroups = length / 3;
        for (int i = 0; i < numGroups; i++) {
            final int byte0 = bytes[index++] & 0xff;
            final int byte1 = bytes[index++] & 0xff;
            final int byte2 = bytes[index++] & 0xff;
            result.append(BASE_64_CHARS[byte0 >> 2]);
            result.append(BASE_64_CHARS[((byte0 << 4) & 0x3f) | (byte1 >> 4)]);
            result.append(BASE_64_CHARS[((byte1 << 2) & 0x3f) | (byte2 >> 6)]);
            result.append(BASE_64_CHARS[byte2 & 0x3f]);
        }

        // Partial group with padding.
        final int numBytesInLastGroup = length - (3 * numGroups);
        if (numBytesInLastGroup > 0) {
            final int byte0 = bytes[index++] & 0xff;
            result.append(BASE_64_CHARS[byte0 >> 2]);
            if (numBytesInLastGroup == 1) {
                result.append(BASE_64_CHARS[(byte0 << 4) & 0x3f]);
                result.append("==");
            }
            else {
                final int byte1 = bytes[index++] & 0xff;
                result.append(BASE_64_CHARS[((byte0 << 4) & 0x3f)
                        | (byte1 >> 4)]);
                result.append(BASE_64_CHARS[(byte1 << 2) & 0x3f]);
                result.append('=');
            }
        }

        return result.toString();
    }

    /**
     * Converts the byte array into a HEX string.
     *
     * @param bytes
     *            The bytes to convert.
     * @return The string version.
     */
    public static String toHex(final byte[] bytes) {
        final StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            builder.append(HEX_CHARS[(b >> 4) & 0xF]);
            builder.append(HEX_CHARS[b & 0xF]);
        }
        return builder.toString();
    }

    /**
     * Uses the provided alphabet to convert the character to a set of bits.
     *
     * @param alphabet
     *            The alphabet for the conversion.
     * @param c
     *            The character to convert.
     * @return The bits for the character from the alphabet.
     * @throws IllegalArgumentException
     *             If the character is not in the alphabet.
     */
    private static int alphabetToBits(final byte[] alphabet, final char c) {
        int v = -1;
        if (c < alphabet.length) {
            v = alphabet[c];
        }
        if (v < 0) {
            throw new IllegalArgumentException(
                    "Invalid character in the encoded string: '" + c + "'.");
        }
        return v;
    }

    /**
     * Stop creation of a new IOUtils.
     */
    private IOUtils() {
        // Nothing.
    }
}
