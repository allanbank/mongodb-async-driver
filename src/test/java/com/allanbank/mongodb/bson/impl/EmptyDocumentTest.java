/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * EmptyDocumentTest provides tests for the {@link EmptyDocument} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("boxing")
public class EmptyDocumentTest {

    /**
     * Test method for
     * {@link EmptyDocument#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final Document doc = new EmptyDocument();
        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visit(EmptyDocument.EMPTY_ELEMENTS);
        expectLastCall();

        replay(mockVisitor);

        doc.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link EmptyDocument#asDocument()}.
     */
    @Test
    public void testAsDocument() {
        final Document doc = new EmptyDocument();
        assertThat(doc.asDocument(), is(doc));
    }

    /**
     * Test method for {@link EmptyDocument#contains(String)} .
     */
    @Test
    public void testContains() {
        assertThat(new EmptyDocument().contains("a"), is(false));
    }

    /**
     * Test method for {@link EmptyDocument#equals} and
     * {@link EmptyDocument#hashCode()}.
     */
    @Test
    public void testEqualsHashCode() {
        final List<Document> objs1 = new ArrayList<Document>();
        final List<Document> objs2 = new ArrayList<Document>();

        objs1.add(new EmptyDocument());
        objs2.add(new EmptyDocument());

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Document obj1 = objs1.get(i);
            Document obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
        }
    }

    /**
     * Test method for {@link EmptyDocument#equals(Object)} .
     */
    @Test
    public void testEqualsRootDocument() {
        final Document root = new RootDocument();
        final Document rootWithElement = new RootDocument(new StringElement(
                "a", "b"));
        final Document empty = new EmptyDocument();

        assertThat(empty.hashCode(), is(root.hashCode()));
        assertThat(empty, is(root));
        assertThat(root, is(empty));

        assertThat(rootWithElement, not(is(empty)));
        assertThat(empty, not(is(rootWithElement)));
    }

    /**
     * Test method for {@link EmptyDocument#find(Class, String[])} .
     */
    @Test
    public void testFindClassOfEStringArray() {
        assertThat(new EmptyDocument().find(Element.class, "a"),
                is(EmptyDocument.EMPTY_ELEMENTS));
    }

    /**
     * Test method for {@link EmptyDocument#findFirst(Class, String[])} .
     */
    @Test
    public void testFindFirstClassOfEStringArray() {
        assertThat(new EmptyDocument().get(Element.class, "a"), nullValue());
    }

    /**
     * Test method for {@link EmptyDocument#findFirst(String[])} .
     */
    @Test
    public void testFindFirstStringArray() {
        assertThat(new EmptyDocument().get("a"), nullValue());
    }

    /**
     * Test method for {@link EmptyDocument#find(String[])} .
     */
    @Test
    public void testFindStringArray() {
        assertThat(new EmptyDocument().find("a"),
                is(EmptyDocument.EMPTY_ELEMENTS));
    }

    /**
     * Test method for {@link EmptyDocument#get(Class, String)} .
     */
    @Test
    public void testGetClassOfEString() {
        assertThat(new EmptyDocument().get(Element.class, null), nullValue());
    }

    /**
     * Test method for {@link EmptyDocument#getElements()}.
     */
    @Test
    public void testGetElements() {
        assertThat(new EmptyDocument().getElements(),
                is(EmptyDocument.EMPTY_ELEMENTS));
    }

    /**
     * Test method for {@link EmptyDocument#get(String)} .
     */
    @Test
    public void testGetString() {
        assertThat(new EmptyDocument().get(null), nullValue());
    }

    /**
     * Test method for {@link EmptyDocument#iterator()}.
     */
    @Test
    public void testIterator() {
        assertThat(new EmptyDocument().iterator().hasNext(), is(false));
    }

    /**
     * Test method for {@link EmptyDocument#queryPath(Class, String[])} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testQueryPathClassOfEStringArray() {
        assertThat(new EmptyDocument().queryPath(Element.class, "a"),
                is(EmptyDocument.EMPTY_ELEMENTS));
    }

    /**
     * Test method for {@link EmptyDocument#queryPath(String[])} .
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testQueryPathStringArray() {
        assertThat(new EmptyDocument().queryPath("a"),
                is(EmptyDocument.EMPTY_ELEMENTS));
    }

    /**
     * Test method for {@link EmptyDocument#toString()}.
     */
    @Test
    public void testToString() {
        assertThat(new EmptyDocument().toString(), is("{}"));
    }

}
