/*
 * #%L
 * ArrayElementTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.bson.element;

import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.Visitor;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.impl.ArrayBuilderImpl;
import org.junit.Test;

import java.util.*;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * ArrayElementTest provides tests for the {@link ArrayElement} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayElementTest {

    /**
     * Test method for
     * {@link ArrayElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAccept() {
        final BooleanElement subElement = new BooleanElement("0", false);
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
     * Test method for
     * {@link ArrayElement#accept(com.allanbank.mongodb.bson.Visitor)} .
     */
    @Test
    public void testAcceptWithSizeAware() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final SizeAwareVisitor mockVisitor = createMock(SizeAwareVisitor.class);

        mockVisitor.visitArray("foo",
                Collections.singletonList((Element) subElement), 14);
        expectLastCall();

        replay(mockVisitor);

        element.accept(mockVisitor);

        verify(mockVisitor);
    }

    /**
     * Test method for {@link ArrayElement#compareTo(Element)}.
     */
    @Test
    public void testCompareTo() {
        final ArrayElement a1 = new ArrayElement("a", new StringElement("a",
                "1"));
        final ArrayElement a11 = new ArrayElement("a", new StringElement("a",
                "1"), new StringElement("a", "1"));
        final ArrayElement a2 = new ArrayElement("a", new StringElement("a",
                "2"));
        final ArrayElement b1 = new ArrayElement("b", new StringElement("a",
                "1"));
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
     * Test method for {@link ArrayElement#ArrayElement(String, Element...)}.
     */
    @Test
    public void testConstructManuallyWithArray() {
        final ArrayElement ae = new ArrayElement("listObj", new StringElement(
                "", "fabse"), new StringElement("", "fabse2"));

        assertThat((StringElement) ae.getEntries().get(0),
                is(new StringElement("0", "fabse")));
        assertThat((StringElement) ae.getEntries().get(1),
                is(new StringElement("1", "fabse2")));
    }

    /**
     * Test method for {@link ArrayElement#ArrayElement(String, Element...)}.
     */
    @Test
    public void testConstructManuallyWithList() {
        final ArrayElement ae = new ArrayElement("listObj", Arrays.asList(
                (Element) new StringElement("", "fabse"), new StringElement("",
                        "fabse2")));

        assertThat((StringElement) ae.getEntries().get(0),
                is(new StringElement("0", "fabse")));
        assertThat((StringElement) ae.getEntries().get(1),
                is(new StringElement("1", "fabse2")));
    }

    /**
     * Test method for {@link ArrayElement#ArrayElement(String, List)}.
     */
    @Test
    public void testConstructors() {
        List<Element> list = Collections.emptyList();
        final BooleanElement subElement = new BooleanElement("0", false);

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
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "(");
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "(");
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithEmptyPathMatchingType() {

        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("1", false));

        final Element found = element.findFirst(Element.class);
        assertNotNull(found);
        assertSame(element, found);

        final ArrayElement arrayFound = element.findFirst(ArrayElement.class);
        assertNotNull(arrayFound);
        assertSame(element, arrayFound);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithEmptyPathNonMatchingType() {
        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("1", false));

        final Element found = element.findFirst(BooleanElement.class);
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonFirstSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("f", new BooleanElement(
                "0", false), subElement);

        Element found = element.findFirst(Element.class, "1");
        assertSame(subElement, found);

        found = element.findFirst(Element.class, "1|2");
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonLastSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("f", subElement,
                new BooleanElement("1", false));

        Element found = element.findFirst(Element.class, "0");
        assertSame(subElement, found);

        found = element.findFirst(Element.class, "0|3");
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingNonLastSubelementBadRegex() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("f", subElement,
                new BooleanElement("1", false));

        Element found = element.findFirst(Element.class, "(");
        assertNull(found);

        found = element.findFirst(Element.class, "(");
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        Element found = element.findFirst(Element.class, "0");
        assertNotNull(found);
        assertSame(subElement, found);

        found = element.findFirst(Element.class, ".");
        assertNotNull(found);
        assertSame(subElement, found);
    }

    /**
     * Test method for {@link ArrayElement#findFirst}.
     */
    @Test
    public void testFindFirstWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final Element found = element.findFirst(Element.class, "n.*");
        assertNull(found);
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithBadPatternPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithBadPatternPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "(");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithEmptyPathMatchingType() {

        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("0", false));

        final List<Element> elements = element.find(Element.class);
        assertEquals(1, elements.size());
        assertSame(element, elements.get(0));

        final List<ArrayElement> arrayElements = element
                .find(ArrayElement.class);
        assertEquals(1, arrayElements.size());
        assertSame(element, arrayElements.get(0));
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithEmptyPathNonMatchingType() {
        final ArrayElement element = new ArrayElement("foo",
                new BooleanElement("1", false));

        final List<BooleanElement> elements = element
                .find(BooleanElement.class);
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithPathMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        List<Element> elements = element.find(Element.class, "0");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));

        elements = element.find(Element.class, ".");
        assertEquals(1, elements.size());
        assertSame(subElement, elements.get(0));
    }

    /**
     * Test method for {@link ArrayElement#find}.
     */
    @Test
    public void testFindWithPathNotMatchingSubelement() {
        final BooleanElement subElement = new BooleanElement("1", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        final List<Element> elements = element.find(Element.class, "n.*");
        assertEquals(0, elements.size());
    }

    /**
     * Test method for {@link ArrayElement#getEntries()}.
     */
    @Test
    public void testGetEntries() {
        final BooleanElement subElement = new BooleanElement("0", false);
        final ArrayElement element = new ArrayElement("foo", subElement);

        assertEquals("foo", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(Collections.singletonList(subElement),
                element.getEntries());
    }

    /**
     * Test method for {@link ArrayElement#nameFor(int)} .
     */
    @Test
    public void testNameFor() {
        assertEquals("-1", ArrayElement.nameFor(-1));
        assertEquals("10000", ArrayElement.nameFor(10000));
    }

    /**
     * Test method for {@link ArrayElement#ArrayElement}.
     */
    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testThrowsOnNullName() {

        new ArrayElement(null, new BooleanElement("b", false));
    }

    /**
     * Test method for {@link ArrayElement#toString()}.
     */
    @Test
    public void testToString() {
        final BooleanElement subElement = new BooleanElement("1", false);

        String ls = System.getProperty("line.separator");

        ArrayElement element = new ArrayElement("foo", subElement, subElement);
        assertEquals("foo : [" + ls + "  false, " + ls + "  false" + ls + "]", element.toString());

        element = new ArrayElement("foo", subElement);
        assertEquals("foo : [ false ]", element.toString());

        element = new ArrayElement("foo", new ArrayElement("0"));
        assertEquals("foo : [" + ls + "  []" + ls + "]", element.toString());

        element = new ArrayElement("foo", new DocumentElement("0"));
        assertEquals("foo : [" + ls + "  {}" + ls + "]", element.toString());
    }

    /**
     * Test method for {@link ArrayElement#getValueAsObject()}.
     */
    @Test
    public void testValueAsObject() {
        final BooleanElement subElement = new BooleanElement("0", false);

        ArrayElement element = new ArrayElement("foo", subElement, subElement);
        assertArrayEquals(
                new Element[]{subElement, subElement.withName("1")},
                element.getValueAsObject());

        element = new ArrayElement("foo", subElement);
        assertArrayEquals(new Element[]{subElement},
                element.getValueAsObject());

        element = new ArrayElement("foo", new ArrayElement("0"));
        assertArrayEquals(new Element[]{new ArrayElement("0")},
                element.getValueAsObject());

        element = new ArrayElement("foo", new DocumentElement("0"));
        assertArrayEquals(new Element[]{new DocumentElement("0")},
                element.getValueAsObject());
    }

    /**
     * Test method for {@link ArrayElement#getValueAsString()}.
     */
    @Test
    public void testValueAsString() {
        final BooleanElement subElement = new BooleanElement("0", false);
        String ls = System.getProperty("line.separator");

        ArrayElement element = new ArrayElement("foo", subElement, subElement);
        assertEquals("[" + ls + "  false, " + ls + "  false" + ls + "]", element.getValueAsString());

        element = new ArrayElement("foo", subElement);
        assertEquals("[ false ]", element.getValueAsString());

        element = new ArrayElement("foo", new ArrayElement("0"));
        assertEquals("[" + ls + "  []" + ls + "]", element.getValueAsString());

        element = new ArrayElement("foo", new DocumentElement("0"));
        assertEquals("[" + ls + "  {}" + ls + "]", element.getValueAsString());
    }

    /**
     * Test method for {@link ArrayElement#withName(String)}.
     */
    @Test
    public void testWithName() {
        final List<Element> list = Collections.emptyList();

        ArrayElement element = new ArrayElement("foo", list);
        element = element.withName("bar");
        assertEquals("bar", element.getName());
        assertEquals(ElementType.ARRAY, element.getType());
        assertEquals(list, element.getEntries());
    }

    /**
     * Test method for {@link ArrayElement#withName(String)}.
     */
    @Test
    public void testWithNameWhenSameName() {
        final List<Element> list = Collections.emptyList();

        final ArrayElement element = new ArrayElement("bar", list);

        assertSame(element, element.withName("bar"));
    }
}
