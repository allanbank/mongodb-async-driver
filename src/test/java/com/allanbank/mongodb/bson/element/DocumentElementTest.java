/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

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

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * DocumentElementTest provides TODO - Finish
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentElementTest {

    /**
     * Test method for
     * {@link DocumentElement#accept(com.allanbank.mongodb.bson.Visitor)}.
     */
    @Test
    public void testAccept() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitDocument("foo",
                Collections.singletonList((Element) subElement));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testConstructEmptyDocument() {
        final DocumentElement element = new DocumentElement("foo");

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testConstructEmptyDocumentList() {
        final DocumentElement element = new DocumentElement("foo",
                (List<Element>) null);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testConstructEmptyDocumentListEmpty() {
        final List<Element> elements = Collections.emptyList();
        final DocumentElement element = new DocumentElement("foo", elements);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(java.lang.String, java.util.List)}
     * .
     */
    @Test
    public void testConstructor() {
        final List<Element> elements = Collections
                .singletonList((Element) new BooleanElement("1", false));
        final DocumentElement element = new DocumentElement("foo", elements);

        assertEquals(elements, element.getElements());
        assertEquals("foo", element.getName());
        assertEquals(ElementType.DOCUMENT, element.getType());
    }

    /**
     * Test method for {@link DocumentElement#contains(java.lang.String)}.
     */
    @Test
    public void testContains() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        assertTrue(element.contains("1"));
        assertFalse(element.contains("2"));
    }

    /**
     * Test method for {@link DocumentElement#equals(java.lang.Object)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsHashCode() {
        final Random rand = new Random(System.currentTimeMillis());
        final ElementType[] types = ElementType.values();

        final List<Element> objs1 = new ArrayList<Element>();
        final List<Element> objs2 = new ArrayList<Element>();

        for (final String name : Arrays.asList("1", "2", "foo", "bar", "baz",
                "2")) {
            final DocumentBuilderImpl builder = new DocumentBuilderImpl();
            for (final String elemName : Arrays.asList("1", "2", "foo", "bar",
                    "baz")) {
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
                    builder.addDBPointer(elemName, "foo", "bar", new ObjectId());
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

            objs1.add(new DocumentElement(name, builder.get()));
            objs2.add(new DocumentElement(name, builder.get()));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Element obj1 = objs1.get(i);
            Element obj2 = objs2.get(i);

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
            assertFalse(obj1.equals(new BooleanElement(obj1.getName(), true)));
        }
    }

    /**
     * Test method for {@link DocumentElement#get(java.lang.String)}.
     */
    @Test
    public void testGet() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        assertSame(subElement, element.get("1"));
        assertNull(element.get("2"));
    }

    /**
     * Test method for {@link DocumentElement#getElements()}.
     */
    @Test
    public void testGetElements() {
        final Element subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        assertEquals(Collections.singletonList(subElement),
                element.getElements());
    }

    /**
     * Test method for {@link DocumentElement#injectId()}.
     */
    @Test
    public void testInjectId() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        assertFalse(element.contains("_id"));

        element.injectId();

        assertTrue(element.contains("_id"));
        assertTrue(element.get("_id") instanceof ObjectIdElement);

        element.injectId();

        assertTrue(element.contains("_id"));
        assertTrue(element.get("_id") instanceof ObjectIdElement);
    }

    /**
     * Test method for {@link DocumentElement#iterator()}.
     */
    @Test
    public void testIterator() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final Iterator<Element> iter = element.iterator();
        assertNotNull(iter);
        assertTrue(iter.hasNext());
        assertSame(subElement, iter.next());
        assertFalse(iter.hasNext());
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "(");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithEmptyPathMatchingType() {

        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final List<Element> elements = element.queryPath(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));

        final List<DocumentElement> arrayElements = element
                .queryPath(DocumentElement.class);
        assertEquals(1, arrayElements.size());
        assertSame(element, arrayElements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithEmptyPathNonMatchingType() {
        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final List<BooleanElement> elements = element
                .queryPath(BooleanElement.class);
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        List<Element> elements = element.queryPath(Element.class, "1");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));

        elements = element.queryPath(Element.class, ".");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#queryPath}.
     */
    @Test
    public void testQueryPathWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "n.*");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#toString()}.
     */
    @Test
    public void testToString() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final BooleanElement subElement2 = new BooleanElement("2", false);
        final DocumentElement element = new DocumentElement("foo", subElement,
                subElement2);

        assertEquals("\"foo\" : { \"1\" : false,\n\"2\" : false}\n",
                element.toString());
    }

}
