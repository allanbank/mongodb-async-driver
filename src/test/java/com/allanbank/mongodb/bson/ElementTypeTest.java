/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * ElementTypeTest provides tests for the elements type.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ElementTypeTest {

    /**
     * Test method for {@link ElementType#valueOf(byte)}.
     */
    @Test
    public void testValueOf() {
        for (final ElementType type : ElementType.values()) {
            assertSame(type, ElementType.valueOf(type.getToken()));
        }
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithMinKey() {

        assertTrue(ElementType.MIN_KEY.compare(ElementType.MIN_KEY) == 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.NULL) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.DOUBLE) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.INTEGER) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.LONG) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.SYMBOL) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.STRING) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.MIN_KEY
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.MIN_KEY.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithNull() {

        assertTrue(ElementType.NULL.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.NULL.compare(ElementType.NULL) == 0);
        assertTrue(ElementType.NULL.compare(ElementType.DOUBLE) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.INTEGER) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.LONG) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.SYMBOL) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.STRING) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.NULL
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.NULL.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithDouble() {

        assertTrue(ElementType.DOUBLE.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.DOUBLE) == 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.INTEGER) == 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.LONG) == 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.SYMBOL) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.STRING) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.DOUBLE
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.DOUBLE.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithInteger() {

        assertTrue(ElementType.INTEGER.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.DOUBLE) == 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.INTEGER) == 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.LONG) == 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.SYMBOL) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.STRING) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.INTEGER
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.INTEGER.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithLong() {

        assertTrue(ElementType.LONG.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.LONG.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.LONG.compare(ElementType.DOUBLE) == 0);
        assertTrue(ElementType.LONG.compare(ElementType.LONG) == 0);
        assertTrue(ElementType.LONG.compare(ElementType.INTEGER) == 0);
        assertTrue(ElementType.LONG.compare(ElementType.SYMBOL) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.STRING) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.LONG
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.LONG.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithSymbol() {

        assertTrue(ElementType.SYMBOL.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.SYMBOL) == 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.STRING) == 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.SYMBOL
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.SYMBOL.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithString() {

        assertTrue(ElementType.STRING.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.STRING.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.STRING.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.STRING.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.STRING.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.STRING.compare(ElementType.SYMBOL) == 0);
        assertTrue(ElementType.STRING.compare(ElementType.STRING) == 0);
        assertTrue(ElementType.STRING.compare(ElementType.DOCUMENT) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.STRING
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.STRING.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithDocument() {

        assertTrue(ElementType.DOCUMENT.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.DOCUMENT) == 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.ARRAY) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.DOCUMENT
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.DOCUMENT.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithBinary() {

        assertTrue(ElementType.BINARY.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.BINARY.compare(ElementType.BINARY) == 0);
        assertTrue(ElementType.BINARY.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.BINARY
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.BINARY.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithObjectId() {

        assertTrue(ElementType.OBJECT_ID.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.OBJECT_ID) == 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.OBJECT_ID
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.OBJECT_ID.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithBoolean() {

        assertTrue(ElementType.BOOLEAN.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.BOOLEAN) == 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.BOOLEAN
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.BOOLEAN.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithTimestamp() {

        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.UTC_TIMESTAMP
                .compare(ElementType.UTC_TIMESTAMP) == 0);
        assertTrue(ElementType.UTC_TIMESTAMP
                .compare(ElementType.MONGO_TIMESTAMP) == 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.UTC_TIMESTAMP
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.UTC_TIMESTAMP.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithMongoTimestamp() {

        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.MONGO_TIMESTAMP
                .compare(ElementType.UTC_TIMESTAMP) == 0);
        assertTrue(ElementType.MONGO_TIMESTAMP
                .compare(ElementType.MONGO_TIMESTAMP) == 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.MONGO_TIMESTAMP
                .compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.MONGO_TIMESTAMP
                .compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.MONGO_TIMESTAMP
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.MONGO_TIMESTAMP.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithDbPointer() {

        assertTrue(ElementType.DB_POINTER.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.UTC_TIMESTAMP) > 0);
        assertTrue(ElementType.DB_POINTER
                .compare(ElementType.MONGO_TIMESTAMP) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.REGEX) > 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.DB_POINTER) == 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.DB_POINTER
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.DB_POINTER.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithRegex() {

        assertTrue(ElementType.REGEX.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.UTC_TIMESTAMP) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.MONGO_TIMESTAMP) > 0);
        assertTrue(ElementType.REGEX.compare(ElementType.REGEX) == 0);
        assertTrue(ElementType.REGEX.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.REGEX.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.REGEX
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.REGEX.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithJavaScript() {

        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.UTC_TIMESTAMP) > 0);
        assertTrue(ElementType.JAVA_SCRIPT
                .compare(ElementType.MONGO_TIMESTAMP) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.REGEX) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.DB_POINTER) > 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.JAVA_SCRIPT) == 0);
        assertTrue(ElementType.JAVA_SCRIPT
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.JAVA_SCRIPT.compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithJavaScriptWithScope() {

        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.NULL) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.LONG) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.STRING) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.UTC_TIMESTAMP) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.MONGO_TIMESTAMP) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.REGEX) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.DB_POINTER) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.JAVA_SCRIPT) > 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) == 0);
        assertTrue(ElementType.JAVA_SCRIPT_WITH_SCOPE
                .compare(ElementType.MAX_KEY) < 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithMaxKey() {

        assertTrue(ElementType.MAX_KEY.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.ARRAY) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.BINARY) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.OBJECT_ID) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.BOOLEAN) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.UTC_TIMESTAMP) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.MONGO_TIMESTAMP) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.REGEX) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.DB_POINTER) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.JAVA_SCRIPT) > 0);
        assertTrue(ElementType.MAX_KEY
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) > 0);
        assertTrue(ElementType.MAX_KEY.compare(ElementType.MAX_KEY) == 0);
    }

    /**
     * Tests method for {@link ElementType#compare(ElementType)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompareWithArray() {
        assertTrue(ElementType.ARRAY.compare(ElementType.MIN_KEY) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.NULL) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.DOUBLE) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.INTEGER) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.LONG) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.SYMBOL) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.STRING) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.DOCUMENT) > 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.ARRAY) == 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.BINARY) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.OBJECT_ID) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.BOOLEAN) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.UTC_TIMESTAMP) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.MONGO_TIMESTAMP) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.REGEX) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.DB_POINTER) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.JAVA_SCRIPT) < 0);
        assertTrue(ElementType.ARRAY
                .compare(ElementType.JAVA_SCRIPT_WITH_SCOPE) < 0);
        assertTrue(ElementType.ARRAY.compare(ElementType.MAX_KEY) < 0);
    }
}
