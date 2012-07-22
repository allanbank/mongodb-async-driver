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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;

/**
 * ArrayElementTest provides tests for the {@link ArrayElement} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayElementTest {

    /**
     * Test method for
     * {@link ArrayElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final Visitor mockVisitor = createMock(Visitor.class);

        mockVisitor.visitArray("foo",
                Collections.singletonList((Element) subElement));
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link ArrayElement#ArrayElement(String, List)}.
     */
    @Test
    public void testConstructors() {
        List<Element> list = Collections.emptyList();
        final BooleanElement subElement = new BooleanElement("1", false);

        ArrayElement element = new ArrayElement("foo", list);

        assertEquals("foo", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(list, element.getEntries());

        element = new ArrayElement("foo", (List<Element>) null);
        assertEquals("foo", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(list, element.getEntries());

        list = Collections.singletonList((Element) subElement);
        element = new ArrayElement("foo", list);
        assertEquals("foo", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(Collections.singletonList(subElement),
                element.getEntries());
        assertNotSame(list, element.getEntries());
    }

    /**
     * Test method for {@link ArrayElement#equals} and
     * {@link ArrayElement#hashCode}
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsHashCode() {
        final Random rand = new Random(System.currentTimeMillis());
        final ElementType[] types = ElementType.values();

        final List<ArrayElement> objs1 = new ArrayList<ArrayElement>();
        final List<ArrayElement> objs2 = new ArrayList<ArrayElement>();

        for (final String name : Arrays.asList("1", "2", "foo", "bar", "baz",
                "2")) {
            final ArrayBuilderImpl builder = new ArrayBuilderImpl();
            final int count = rand.nextInt(50);
            for (int i = 0; i < count; ++i) {
                final ElementType type = types[rand.nextInt(types.length)];
                switch (type) {
                case ARRAY:
                    builder.pushArray().addBoolean(rand.nextBoolean());
                    break;
                case BINARY:
                    final byte[] bytes = new byte[rand.nextInt(17)];
                    rand.nextBytes(bytes);
                    builder.addBinary(bytes);
                    break;
                case BOOLEAN:
                    builder.addBoolean(rand.nextBoolean());
                    break;
                case DB_POINTER:
                    builder.addDBPointer("foo", "bar", new ObjectId());
                    break;
                case DOCUMENT:
                    builder.push().addBoolean("boolean", rand.nextBoolean());
                    break;
                case DOUBLE:
                    builder.addDouble(rand.nextDouble());
                    break;
                case INTEGER:
                    builder.addInteger(rand.nextInt());
                    break;
                case JAVA_SCRIPT:
                    builder.addJavaScript("function foo() { }");
                    break;
                case JAVA_SCRIPT_WITH_SCOPE:
                    builder.addJavaScript("function bar() {}", BuilderFactory
                            .start().build());
                    break;
                case LONG:
                    builder.addLong(rand.nextLong());
                    break;
                case MAX_KEY:
                    builder.addMaxKey();
                    break;
                case MIN_KEY:
                    builder.addMinKey();
                    break;
                case MONGO_TIMESTAMP:
                    builder.addMongoTimestamp(System.currentTimeMillis());
                    break;
                case NULL:
                    builder.addNull();
                    break;
                case OBJECT_ID:
                    builder.addObjectId(new ObjectId());
                    break;
                case REGEX:
                    builder.addRegularExpression(".*", "");
                    break;
                case STRING:
                    builder.addString("" + rand.nextInt());
                    break;
                case SYMBOL:
                    builder.addSymbol("" + rand.nextInt());
                    break;
                case UTC_TIMESTAMP:
                    builder.addTimestamp(System.currentTimeMillis());
                    break;

                }
            }

            objs1.add(builder.build(name));
            objs2.add(builder.build(name));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final ArrayElement obj1 = objs1.get(i);
            ArrayElement obj2 = objs2.get(i);

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
     * Test method for {@link ArrayElement#getEntries()}.
     */
    @Test
    public void testGetEntries() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        assertEquals("foo", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(Collections.singletonList(subElement),
                element.getEntries());
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("(", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "(");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithEmptyPathMatchingType() {

        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("1", false));

        final List<Element> elements = element.queryPath(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));

        final List<ArrayElement> arrayElements = element
                .queryPath(ArrayElement.class);
        assertEquals(1, arrayElements.size());
        assertSame(element, arrayElements.get(0));
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithEmptyPathNonMatchingType() {
        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("1", false));

        final List<BooleanElement> elements = element
                .queryPath(BooleanElement.class);
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        List<Element> elements = element.queryPath(Element.class, "1");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));

        elements = element.queryPath(Element.class, ".");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link ArrayElement#queryPath}.
     */
    @Test
    public void testQueryPathWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.queryPath(Element.class, "n.*");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#toString()}.
     */
    @Test
    public void testToString() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement,
                subElement);

        assertEquals("\"foo\" : [ \"1\" : false,\n\"1\" : false]\n",
                element.toString());
    }
}
