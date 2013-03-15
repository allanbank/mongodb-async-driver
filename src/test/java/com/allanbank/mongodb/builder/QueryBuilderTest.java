/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.QueryBuilder.and;
import static com.allanbank.mongodb.builder.QueryBuilder.nor;
import static com.allanbank.mongodb.builder.QueryBuilder.not;
import static com.allanbank.mongodb.builder.QueryBuilder.or;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * QueryBuilderTest provides tests for the {@link QueryBuilder} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryBuilderTest {
    /**
     * Test method for {@link QueryBuilder#and(DocumentAssignable[])} .
     */
    @Test
    public void testAndWithMultipleEntry() {
        final Document doc = and(where("x").equals(23), where("y").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);
        expected.addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#and(DocumentAssignable[])} .
     */
    @Test
    public void testAndWithMultipleEntryDuplicated() {
        final Document doc = and(where("x").equals(23), where("y").equals(23),
                where("x").equals(13), where("y").equals(13));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.AND
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);
        ab.push().addInteger("x", 13);
        ab.push().addInteger("y", 13);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#and(DocumentAssignable[])} .
     */
    @Test
    public void testAndWithMultipleEntryLastNoCriteria() {
        final Document doc = and(where("x").equals(23), where("y").equals(23),
                where("z"), where("y").equals(13));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.AND
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);
        ab.push().addInteger("y", 13);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#and(DocumentAssignable[])} .
     */
    @Test
    public void testAndWithNoEntries() {
        final Document doc = and();

        final DocumentAssignable expected = BuilderFactory.start();

        assertEquals(expected.asDocument(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#and(DocumentAssignable[])} .
     */
    @Test
    public void testAndWithOneEntry() {
        final Document doc = and(where("x").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#asDocument()}.
     */
    @Test
    public void testAsDocument() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        final Document doc = builder.asDocument();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#build()}.
     */
    @Test
    public void testBuild() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        final Document doc = builder.build();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#build()}.
     */
    @Test
    public void testBuildOneWithNoCriteria() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        builder.whereField("z");
        final Document doc = builder.build();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#nor(DocumentAssignable[])} .
     */
    @Test
    public void testNor() {
        final Document doc = nor(where("x").equals(23), where("y").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.NOR
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#nor(DocumentAssignable[])} .
     */
    @Test
    public void testNorOneWithNoCriteria() {
        final Document doc = nor(where("x").equals(23), where("y").equals(23),
                where("z"));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.NOR
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#not(DocumentAssignable[])} .
     */
    @Test
    public void testNot() {
        final Document doc = not(where("x").equals(23), where("y").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.NOT
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#not(DocumentAssignable[])} .
     */
    @Test
    public void testNotOneWithNoCriteria() {
        final Document doc = not(where("x").equals(23), where("y").equals(23),
                where("z"));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.NOT
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#or(DocumentAssignable[])} .
     */
    @Test
    public void testOr() {
        final Document doc = or(where("x").equals(23), where("y").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.OR
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#or(DocumentAssignable[])} .
     */
    @Test
    public void testOrNoEntries() {
        final Document doc = or();

        final DocumentBuilder expected = BuilderFactory.start();

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#or(DocumentAssignable[])} .
     */
    @Test
    public void testOrOneEntry() {
        final Document doc = or(where("x").equals(23));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#or(DocumentAssignable[])} .
     */
    @Test
    public void testOrOneWithoutCriteria() {
        final Document doc = or(where("x").equals(23), where("y").equals(23),
                where("z"));

        final DocumentBuilder expected = BuilderFactory.start();
        final ArrayBuilder ab = expected.pushArray(LogicalOperator.OR
                .getToken());
        ab.push().addInteger("x", 23);
        ab.push().addInteger("y", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#reset()}.
     */
    @Test
    public void testReset() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        Document doc = builder.asDocument();

        DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);

        builder.reset();
        doc = builder.asDocument();

        expected = BuilderFactory.start();

        assertEquals(expected.build(), doc);

    }

    /**
     * Simple test for the usability of the {@link QueryBuilder}. Testing ground
     * for syntax and structure.
     */
    @Test
    public void testUsability() {
        final Document doc = and(where("x").equals(23).and("y")
                .greaterThanOrEqualTo(44));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);
        expected.push("y").addInteger("$gte", 44);

        assertEquals("The query is not the expected document.",
                expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#where(String)} .
     */
    @Test
    public void testWhere() {
        final Document doc = where("x").equals(23).build();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#whereField(String)} .
     */
    @Test
    public void testWhereField() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        final Document doc = builder.asDocument();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#whereField(String)} .
     */
    @Test
    public void testWhereFieldAlreadyExists() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        builder.whereField("x").equals(21);
        final Document doc = builder.asDocument();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 21);

        assertEquals(expected.build(), doc);
    }

    /**
     * Test method for {@link QueryBuilder#whereJavaScript(String)} .
     */
    @Test
    public void testWhereJavaScript() {
        final QueryBuilder builder = new QueryBuilder();
        builder.whereField("x").equals(23);
        builder.whereJavaScript("f == g");
        final Document doc = builder.asDocument();

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("x", 23);
        expected.addJavaScript(MiscellaneousOperator.WHERE.getToken(), "f == g");

        assertEquals(expected.build(), doc);
    }

}
