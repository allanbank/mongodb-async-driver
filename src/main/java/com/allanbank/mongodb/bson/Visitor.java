/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.bson;

import java.util.List;

import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * Interface for callbacks to navigate the document structure. The accept method
 * of each {@link Element} calls the appropriate Visit method in this interface
 * The user is responsible for recursively navigating the structure.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Visitor {

    /**
     * Visit the root document.
     * 
     * @param elements
     *            The sub elements of the document.
     */
    public void visit(List<Element> elements);

    /**
     * Visits an array of elements.
     * 
     * @param name
     *            The name of the element.
     * @param elements
     *            The elements in the array.
     */
    public void visitArray(String name, List<Element> elements);

    /**
     * Visits a binary element.
     * 
     * @param name
     *            The name of the element.
     * @param subType
     *            The binary data sub type.
     * @param data
     *            The binary data.
     */
    public void visitBinary(String name, byte subType, byte[] data);

    /**
     * Visits a boolean element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The boolean value.
     */
    public void visitBoolean(String name, boolean value);

    /**
     * Visits a deprecated DBPointer element.
     * 
     * @param name
     *            The name of the element.
     * @param databaseName
     *            The name of the database containing the document.
     * @param collectionName
     *            The name of the collection containing the document.
     * @param id
     *            The id for the document.
     */
    public void visitDBPointer(String name, String databaseName,
            String collectionName, ObjectId id);

    /**
     * Visits a sub-document element.
     * 
     * @param name
     *            The name of the element.
     * @param elements
     *            The sub elements of the document.
     */
    public void visitDocument(String name, List<Element> elements);

    /**
     * Visits a double element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The double value.
     */
    public void visitDouble(String name, double value);

    /**
     * Visits a integer (32-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The integer value.
     */
    public void visitInteger(String name, int value);

    /**
     * Visits a JavaScript element.
     * 
     * @param name
     *            The name of the element.
     * @param code
     *            The java script code.
     */
    public void visitJavaScript(String name, String code);

    /**
     * Visits a JavaScript with Scope element.
     * 
     * @param name
     *            The name of the element.
     * @param code
     *            The java script code.
     * @param scope
     *            The scope for the JacaScript code.
     */
    public void visitJavaScript(String name, String code, Document scope);

    /**
     * Visits a long (64-bit signed) element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The long value.
     */
    public void visitLong(String name, long value);

    /**
     * Visits a minimum key value element. Used as an absolute upper bounds.
     * 
     * @param name
     *            The name of the element.
     */
    public void visitMaxKey(String name);

    /**
     * Visits a minimum key value element. Used as an absolute lower bounds.
     * 
     * @param name
     *            The name of the element.
     */
    public void visitMinKey(String name);

    /**
     * Visits a MongoDB Timestamp element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The mongoDB timstamp value.
     */
    public void visitMongoTimestamp(String name, long value);

    /**
     * Visits a <code>null</code> valued element.
     * 
     * @param name
     *            The name of the element.
     */
    public void visitNull(String name);

    /**
     * Visits an ObjectId element.
     * 
     * @param name
     *            The name of the element.
     * @param id
     *            The object id.
     */
    public void visitObjectId(String name, ObjectId id);

    /**
     * Visits a regular expression element.
     * 
     * @param name
     *            The name of the element.
     * @param pattern
     *            The pattern for the regular expression.
     * @param options
     *            The regular expression options. See the BSON specification for
     *            details.
     */
    public void visitRegularExpression(String name, String pattern,
            String options);

    /**
     * Visits a string element.
     * 
     * @param name
     *            The name of the element.
     * @param value
     *            The string value.
     */
    public void visitString(String name, String value);

    /**
     * Visits a symbol element.
     * 
     * @param name
     *            The name of the element.
     * @param symbol
     *            The symbol value.
     */
    public void visitSymbol(String name, String symbol);

    /**
     * Visits a timestamp element. The timestamp is the number of milliseconds
     * since the Unix epoch.
     * 
     * @param name
     *            The name of the element.
     * @param timestamp
     *            The number of milliseconds since the Unix epoch.
     */
    public void visitTimestamp(String name, long timestamp);
}
