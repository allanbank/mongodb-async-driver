/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.element;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.DocumentReference;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;

/**
 * DocumentElementTest provides tests for the {@link DocumentElement} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
     * Test method for {@link DocumentElement#asDocumentReference()}.
     */
    @Test
    public void testAsDocumentReference() {
        final Element other = new StringElement("other", "other");

        final Element cString = new StringElement(
                DocumentReference.COLLECTION_FIELD_NAME, "c");
        final Element cSymbol = new SymbolElement(
                DocumentReference.COLLECTION_FIELD_NAME, "c");
        final Element cInt = new IntegerElement(
                DocumentReference.COLLECTION_FIELD_NAME, 1);

        final Element idString = new StringElement(
                DocumentReference.ID_FIELD_NAME, "id");
        final Element idSymbol = new SymbolElement(
                DocumentReference.ID_FIELD_NAME, "id");
        final Element idInt = new IntegerElement(
                DocumentReference.ID_FIELD_NAME, 1);

        final Element dString = new StringElement(
                DocumentReference.DATABASE_FIELD_NAME, "d");
        final Element dSymbol = new SymbolElement(
                DocumentReference.DATABASE_FIELD_NAME, "d");
        final Element dInt = new IntegerElement(
                DocumentReference.DATABASE_FIELD_NAME, 1);

        final DocumentReference cIdString = new DocumentReference("c", idString);
        final DocumentReference cIdSymbol = new DocumentReference("c", idSymbol);
        final DocumentReference cIdInt = new DocumentReference("c", idInt);

        final DocumentReference dcIdString = new DocumentReference("d", "c",
                idString);
        final DocumentReference dcIdSymbol = new DocumentReference("d", "c",
                idSymbol);
        final DocumentReference dcIdInt = new DocumentReference("d", "c", idInt);

        assertNull(new DocumentElement("e", cString, idString, dSymbol, other)
                .asDocumentReference());

        assertEquals(cIdString,
                new DocumentElement("e", cString, idString)
                        .asDocumentReference());
        assertEquals(cIdSymbol,
                new DocumentElement("e", cString, idSymbol)
                        .asDocumentReference());
        assertEquals(cIdInt,
                new DocumentElement("e", cString, idInt).asDocumentReference());
        assertNull(new DocumentElement("e", cString, other)
                .asDocumentReference());
        assertEquals(cIdString,
                new DocumentElement("e", cSymbol, idString)
                        .asDocumentReference());
        assertEquals(cIdSymbol,
                new DocumentElement("e", cSymbol, idSymbol)
                        .asDocumentReference());
        assertEquals(cIdInt,
                new DocumentElement("e", cSymbol, idInt).asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idInt).asDocumentReference());
        assertNull(new DocumentElement("e", cInt, other).asDocumentReference());
        assertNull(new DocumentElement("e", other, idString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idInt).asDocumentReference());

        assertEquals(dcIdString, new DocumentElement("e", cString, idString,
                dString).asDocumentReference());
        assertEquals(dcIdSymbol, new DocumentElement("e", cString, idSymbol,
                dString).asDocumentReference());
        assertEquals(dcIdInt,
                new DocumentElement("e", cString, idInt, dString)
                        .asDocumentReference());
        assertNull(new DocumentElement("e", cString, other, dString)
                .asDocumentReference());
        assertEquals(dcIdString, new DocumentElement("e", cSymbol, idString,
                dString).asDocumentReference());
        assertEquals(dcIdSymbol, new DocumentElement("e", cSymbol, idSymbol,
                dString).asDocumentReference());
        assertEquals(dcIdInt,
                new DocumentElement("e", cSymbol, idInt, dString)
                        .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, other, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idString, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idSymbol, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idInt, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, other, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idString, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idSymbol, dString)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idInt, dString)
                .asDocumentReference());

        assertEquals(dcIdString, new DocumentElement("e", cString, idString,
                dSymbol).asDocumentReference());
        assertEquals(dcIdSymbol, new DocumentElement("e", cString, idSymbol,
                dSymbol).asDocumentReference());
        assertEquals(dcIdInt,
                new DocumentElement("e", cString, idInt, dSymbol)
                        .asDocumentReference());
        assertNull(new DocumentElement("e", cString, other, dSymbol)
                .asDocumentReference());
        assertEquals(dcIdString, new DocumentElement("e", cSymbol, idString,
                dSymbol).asDocumentReference());
        assertEquals(dcIdSymbol, new DocumentElement("e", cSymbol, idSymbol,
                dSymbol).asDocumentReference());
        assertEquals(dcIdInt,
                new DocumentElement("e", cSymbol, idInt, dSymbol)
                        .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, other, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idString, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idSymbol, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idInt, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, other, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idString, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idSymbol, dSymbol)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idInt, dSymbol)
                .asDocumentReference());

        assertNull(new DocumentElement("e", cString, idString, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cString, idSymbol, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cString, idInt, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cString, other, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idString, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idSymbol, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idInt, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, other, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idString, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idSymbol, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idInt, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, other, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idString, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idSymbol, dInt)
                .asDocumentReference());
        assertNull(new DocumentElement("e", other, idInt, dInt)
                .asDocumentReference());

        assertNull(new DocumentElement("e", cString, idString, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cString, idSymbol, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cString, idInt, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idString, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idSymbol, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cSymbol, idInt, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idString, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idSymbol, other)
                .asDocumentReference());
        assertNull(new DocumentElement("e", cInt, idInt, other)
                .asDocumentReference());

    }

    /**
     * Test method for {@link DocumentElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final DocumentElement a1 = new DocumentElement("a", new StringElement(
                "a", "1"));
        final DocumentElement a11 = new DocumentElement("a", new StringElement(
                "a", "1"), new StringElement("a", "1"));
        final DocumentElement a2 = new DocumentElement("a", new StringElement(
                "a", "2"));
        final DocumentElement b1 = new DocumentElement("b", new StringElement(
                "a", "1"));
        final Element other = new MaxKeyElement("a");

        assertEquals(0, a1.compareTo(a1));
        assertTrue(a1.compareTo(a11) < 0);
        assertTrue(a11.compareTo(a1) > 0);
        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(a1) > 0);
        assertTrue(a1.compareTo(b1) < 0);
        assertTrue(b1.compareTo(a1) > 0);
        assertTrue(a1.compareTo(other) < 0);
        assertTrue(other.compareTo(a1) > 0);
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocument() {
        final DocumentElement element = new DocumentElement("foo");

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, Collection)} .
     */
    @Test
    public void testConstructEmptyDocumentCollection() {
        final DocumentElement element = new DocumentElement("foo",
                (Collection<Element>) null);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocumentCollectionEmpty() {
        final Collection<Element> elements = Collections.emptyList();
        final DocumentElement element = new DocumentElement("foo", elements);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocumentList() {
        final DocumentElement element = new DocumentElement("foo",
                (List<Element>) null);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.List)} .
     */
    @Test
    public void testConstructEmptyDocumentListEmpty() {
        final List<Element> elements = Collections.emptyList();
        final DocumentElement element = new DocumentElement("foo", elements);

        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.List)} .
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
     * Test method for
     * {@link DocumentElement#DocumentElement(String, java.util.Collection)} .
     */
    @Test
    public void testConstructorWithCollection() {
        final Collection<Element> elements = Collections
                .singletonList((Element) new BooleanElement("1", false));
        final DocumentElement element = new DocumentElement("foo", elements);

        assertEquals(elements, element.getElements());
        assertEquals("foo", element.getName());
        assertEquals(ElementType.DOCUMENT, element.getType());
    }

    /**
     * Test method for {@link DocumentElement#contains(String)}.
     */
    @Test
    public void testContains() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        assertTrue(element.contains("1"));
        assertFalse(element.contains("2"));
    }

    /**
     * Test method for {@link DocumentElement#equals(Object)}.
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
                            BuilderFactory.start().build());
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

            objs1.add(new DocumentElement(name, builder.build()));
            objs2.add(new DocumentElement(name, builder.build()));
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
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "(");
        assertNotNull(found);
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "(");
        assertNull(found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithEmptyPathMatchingType() {

        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final Element found = element.findFirst(Element.class);
        assertNotNull(found);
        assertSame(element, found);

        final DocumentElement arrayFound = element
                .findFirst(DocumentElement.class);
        assertNotNull(arrayFound);
        assertSame(element, arrayFound);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithEmptyPathNonMatchingType() {
        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final Element found = element.findFirst(BooleanElement.class);
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonFirstSubelement() {
        final BooleanElement subElement = new BooleanElement("123", false);
        final DocumentElement element = new DocumentElement("f",
                new BooleanElement("2", false), subElement);

        Element found = element.findFirst(Element.class, "123");
        assertSame(subElement, found);

        found = element.findFirst(Element.class, "12.");
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonLastSubelement() {
        final BooleanElement subElement = new BooleanElement("123", false);
        final DocumentElement element = new DocumentElement("f", subElement,
                new BooleanElement("2", false));

        Element found = element.findFirst(Element.class, "123");
        assertSame(subElement, found);

        found = element.findFirst(Element.class, "12.");
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonLastSubelementBadRegex() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final DocumentElement element = new DocumentElement("f", subElement,
                new BooleanElement("2", false));

        Element found = element.findFirst(Element.class, "(");
        assertSame(subElement, found);

        found = element.findFirst(Element.class, "(");
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        Element found = element.findFirst(Element.class, "1");
        assertNotNull(found);
        assertSame(subElement, found);

        found = element.findFirst(Element.class, ".");
        assertNotNull(found);
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link DocumentElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "n.*");
        assertNull(found);
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "(");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithEmptyPathMatchingType() {

        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final List<Element> elements = element.find(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));

        final List<DocumentElement> arrayElements = element
                .find(DocumentElement.class);
        assertEquals(1, arrayElements.size());
        assertSame(element, arrayElements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithEmptyPathNonMatchingType() {
        final DocumentElement element = new DocumentElement("foo",
                new BooleanElement("1", false));

        final List<BooleanElement> elements = element
                .find(BooleanElement.class);
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        List<Element> elements = element.find(Element.class, "1");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));

        elements = element.find(Element.class, ".");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link DocumentElement#find}.
     */
    @Test
    public void testFindWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "n.*");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link DocumentElement#get(String)}.
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
     * Test method for {@link DocumentElement#get(Class,String)}.
     */
    @Test
    public void testGetWithType() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final DocumentElement element = new DocumentElement("f", subElement);

        assertSame(subElement, element.get(BooleanElement.class, "1"));
        assertNull(element.get(IntegerElement.class, "1"));
        assertNull(element.get(BooleanElement.class, "2"));
    }

    /**
     * Test method for {@link DocumentElement#isDocumentReference()}.
     */
    @Test
    public void testIsDocumentReference() {
        final Element other = new StringElement("other", "other");

        final Element cString = new StringElement(
                DocumentReference.COLLECTION_FIELD_NAME, "c");
        final Element cSymbol = new SymbolElement(
                DocumentReference.COLLECTION_FIELD_NAME, "c");
        final Element cInt = new IntegerElement(
                DocumentReference.COLLECTION_FIELD_NAME, 1);

        final Element idString = new StringElement(
                DocumentReference.ID_FIELD_NAME, "id");
        final Element idSymbol = new SymbolElement(
                DocumentReference.ID_FIELD_NAME, "id");
        final Element idInt = new IntegerElement(
                DocumentReference.ID_FIELD_NAME, 1);

        final Element dString = new StringElement(
                DocumentReference.DATABASE_FIELD_NAME, "d");
        final Element dSymbol = new SymbolElement(
                DocumentReference.DATABASE_FIELD_NAME, "d");
        final Element dInt = new IntegerElement(
                DocumentReference.DATABASE_FIELD_NAME, 1);

        assertFalse(new DocumentElement("e", cString, idString, dSymbol, other)
                .isDocumentReference());

        assertTrue(new DocumentElement("e", cString, idString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, other)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idInt).isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, other).isDocumentReference());
        assertFalse(new DocumentElement("e", other, idString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idInt)
                .isDocumentReference());

        assertTrue(new DocumentElement("e", cString, idString, dString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idSymbol, dString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idInt, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, other, dString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idString, dString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idSymbol, dString)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idInt, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, other, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idString, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idSymbol, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idInt, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, other, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idString, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idSymbol, dString)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idInt, dString)
                .isDocumentReference());

        assertTrue(new DocumentElement("e", cString, idString, dSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idSymbol, dSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cString, idInt, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, other, dSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idString, dSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idSymbol, dSymbol)
                .isDocumentReference());
        assertTrue(new DocumentElement("e", cSymbol, idInt, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, other, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idString, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idSymbol, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idInt, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, other, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idString, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idSymbol, dSymbol)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idInt, dSymbol)
                .isDocumentReference());

        assertFalse(new DocumentElement("e", cString, idString, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, idSymbol, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, idInt, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, other, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idString, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idSymbol, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idInt, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, other, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idString, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idSymbol, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idInt, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, other, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idString, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idSymbol, dInt)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", other, idInt, dInt)
                .isDocumentReference());

        assertFalse(new DocumentElement("e", cString, idString, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, idSymbol, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cString, idInt, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idString, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idSymbol, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cSymbol, idInt, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idString, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idSymbol, other)
                .isDocumentReference());
        assertFalse(new DocumentElement("e", cInt, idInt, other)
                .isDocumentReference());

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
     * Test method for {@link DocumentElement#toString()}.
     */
    @Test
    public void testToString() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final BooleanElement subElement2 = new BooleanElement("2", false);
        final DocumentElement element = new DocumentElement("foo", subElement,
                subElement2);

        assertEquals("foo : {\n  '1' : false,\n  '2' : false\n}",
                element.toString());
    }

    /**
     * Test method for {@link DocumentElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final BooleanElement subElement2 = new BooleanElement("2", false);
        final DocumentElement element = new DocumentElement("foo", subElement,
                subElement2);

        assertEquals(BuilderFactory.start(element).build(),
                element.getValueAsObject());
    }

    /**
     * Test method for {@link DocumentElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final BooleanElement subElement2 = new BooleanElement("2", false);
        final DocumentElement element = new DocumentElement("foo", subElement,
                subElement2);

        assertEquals("{\n  '1' : false,\n  '2' : false\n}",
                element.getValueAsString());
    }

    /**
     * Test method for {@link DocumentElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        DocumentElement element = new DocumentElement("foo");

        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertTrue(element.getElements().isEmpty());
    }

    /**
     * Test method for {@link DocumentElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final DocumentElement element = new DocumentElement("foo");

        assertSame(element, element.withName("foo"));
    }
}
