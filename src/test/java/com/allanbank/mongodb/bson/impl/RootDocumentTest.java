/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.impl;

import static junit.framework.Assert.assertEquals;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.ObjectIdElement;

/**
 * RootDocumentTest provides tests for the {@link RootDocument}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class RootDocumentTest {

    /**
     * Test method for
     * {@link RootDocument#accept(com.allanbank.mongodb.bson.Visitor)}.
     */
    @Test
    public void testAccept() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visit(Collections.singletonList((Element) subElement));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link RootDocument#RootDocument(java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocument() {
        final RootDocument element = new RootDocument();

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for {@link RootDocument#RootDocument(java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocumentList() {
        final RootDocument element = new RootDocument((List<Element>) null);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for {@link RootDocument#RootDocument(java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocumentListEmpty() {
        final List<Element> elements = Collections.emptyList();
        final RootDocument element = new RootDocument(elements);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for {@link RootDocument#RootDocument(java.util.List)} .
     */
    @Test
    public void testConstructor() {
        final List<Element> elements = Collections
                .singletonList((Element) new BooleanElement("1", false));
        final RootDocument element = new RootDocument(elements);

        assertEquals(elements, element.getElements());
    }

    /**
     * Test method for {@link RootDocument#contains(java.lang.String)}.
     */
    @Test
    public void testContains() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        assertTrue(element.contains("1"));
        assertFalse(element.contains("2"));
    }

    /**
     * Test method for {@link RootDocument#equals(java.lang.Object)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsHashCode() {
        final Random rand = new Random(System.currentTimeMillis());
        final ElementType[] types = ElementType.values();

        final List<Document> objs1 = new ArrayList<Document>();
        final List<Document> objs2 = new ArrayList<Document>();

        for (int i = 0; i < 10; ++i) {
            final DocumentBuilderImpl builder = new DocumentBuilderImpl();
            for (final String elemName : Arrays.asList("1", "2", "bar", "baz")) {
                final ElementType type = types[rand.nextInt(types.length)];
                switch (type) {
                case ARRAY:
                    builder.pushArray(elemName).addBoolean(rand.nextBoolean());
                    break;
                case BINARY:
                    final byte[] bytes = new byte[rand.nextInt(17)];
                    rand.nextBytes(bytes);
                    builder.addBinary(elemName, bytes);
                    break;
                case BOOLEAN:
                    builder.addBoolean(elemName, rand.nextBoolean());
                    break;
                case DB_POINTER:
                    builder.addDBPointer(elemName, "bar", "baz", new ObjectId());
                    break;
                case DOCUMENT:
                    builder.push(elemName).addBoolean("boolean",
                            rand.nextBoolean());
                    break;
                case DOUBLE:
                    builder.addDouble(elemName, rand.nextDouble());
                    break;
                case INTEGER:
                    builder.addInteger(elemName, rand.nextInt());
                    break;
                case JAVA_SCRIPT:
                    builder.addJavaScript(elemName, "function foo() { }");
                    break;
                case JAVA_SCRIPT_WITH_SCOPE:
                    builder.addJavaScript(elemName, "function bar() {}",
                            BuilderFactory.start().get());
                    break;
                case LONG:
                    builder.addLong(elemName, rand.nextLong());
                    break;
                case MAX_KEY:
                    builder.addMaxKey(elemName);
                    break;
                case MIN_KEY:
                    builder.addMinKey(elemName);
                    break;
                case MONGO_TIMESTAMP:
                    builder.addMongoTimestamp(elemName,
                            System.currentTimeMillis());
                    break;
                case NULL:
                    builder.addNull(elemName);
                    break;
                case OBJECT_ID:
                    builder.addObjectId(elemName, new ObjectId());
                    break;
                case REGEX:
                    builder.addRegularExpression(elemName, ".*", "");
                    break;
                case STRING:
                    builder.addString(elemName, "" + rand.nextInt());
                    break;
                case SYMBOL:
                    builder.addSymbol(elemName, "" + rand.nextInt());
                    break;
                case UTC_TIMESTAMP:
                    builder.addTimestamp(elemName, System.currentTimeMillis());
                    break;

                }
            }

            objs1.add(builder.get());
            objs2.add(builder.get());
        }

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
     * Test method for {@link RootDocument#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        assertSame(subElement, element.get("1"));
        assertNull(element.get("2"));
    }

    /**
     * Test method for {@link RootDocument#getElements()}.
     */
    @Test
    public void testGetElements() {
        final Element subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        assertEquals(Collections.singletonList(subElement),
                element.getElements());
    }

    /**
     * Test method for {@link RootDocument#injectId()}.
     */
    @Test
    public void testInjectId() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        assertFalse(element.contains("_id"));

        element.injectId();

        assertTrue(element.contains("_id"));
        assertTrue(element.get("_id") instanceof ObjectIdElement);

        element.injectId();

        assertTrue(element.contains("_id"));
        assertTrue(element.get("_id") instanceof ObjectIdElement);
    }

    /**
     * Test method for {@link RootDocument#injectId()}.
     */
    @Test
    public void testInjectIdWhenLiedTo() {
        final Element subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(
                Collections.singletonList(subElement), true);

        assertTrue(element.contains("_id"));
        assertTrue(element.contains("1"));
        assertFalse(element.contains("2"));

        element.injectId();

        assertTrue(element.contains("_id"));
        assertNull(element.get("_id"));

        element.injectId();

        assertTrue(element.contains("_id"));
        assertNull(element.get("_id"));
    }

    /**
     * Test method for {@link RootDocument#iterator()}.
     */
    @Test
    public void testIterator() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        final Iterator<Element> iter = element.iterator();
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertSame(subElement, iter.next());
        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link RootDocument#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final RootDocument element = new RootDocument(subElement);

        final List<Element> elements = element.queryPath("(");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link RootDocument#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        final List<Element> elements = element.queryPath(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link RootDocument#queryPath}.
     */
    @Test
    public void testQueryPathWithEmptyPathNonMatchingType() {
        final RootDocument element = new RootDocument(new BooleanElement("1",
                false));

        final List<BooleanElement> elements = element
                .queryPath(BooleanElement.class);
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link RootDocument#queryPath}.
     */
    @Test
    public void testQueryPathWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        List<Element> elements = element.queryPath(Element.class, "1");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));

        elements = element.queryPath(Element.class, ".");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link RootDocument#queryPath}.
     */
    @Test
    public void testQueryPathWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final RootDocument element = new RootDocument(subElement);

        final List<Element> elements = element.queryPath(Element.class, "n.*");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link RootDocument#toString()}.
     */
    @Test
    public void testToString() {
        final Element subElement = new BooleanElement("1", false);
        final Element subElement2 = new BooleanElement("2", false);
        final RootDocument element = new RootDocument(Arrays.asList(subElement,
                subElement2));

        assertEquals("{ \"1\" : false,\n\"2\" : false}\n", element.toString());
    }

}