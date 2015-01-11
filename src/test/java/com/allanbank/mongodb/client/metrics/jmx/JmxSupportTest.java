/*
 * #%L
 * JmxSupportTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.metrics.jmx;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.client.metrics.AbstractMetrics;

/**
 * JmxSupportTest provides tests for the {@link JmxSupport} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxSupportTest {

    /**
     * Test method for
     * {@link JmxSupport#register(AbstractMetrics, String, String)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testRegister() throws JMException {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<Object> captureObject = new Capture<Object>();
        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(
                mockServer.registerMBean(capture(captureObject),
                        capture(captureName))).andReturn(null);

        replay(mockMetrics, mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.register(mockMetrics, "subType", "foo");

        verify(mockMetrics, mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("name"), is("foo"));
        assertThat(name.getKeyProperty("index"), nullValue());

        final Object bean = captureObject.getValue();
        assertThat(bean, instanceOf(MetricsMXBeanProxy.class));
    }

    /**
     * Test method for
     * {@link JmxSupport#register(AbstractMetrics, String, String)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testRegisterOnJMException() throws JMException {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<Object> captureObject = new Capture<Object>();
        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(
                mockServer.registerMBean(capture(captureObject),
                        capture(captureName))).andThrow(
                new InstanceAlreadyExistsException());

        replay(mockMetrics, mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.register(mockMetrics, "subType", "foo");

        verify(mockMetrics, mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("name"), is("foo"));
        assertThat(name.getKeyProperty("index"), nullValue());

        final Object bean = captureObject.getValue();
        assertThat(bean, instanceOf(MetricsMXBeanProxy.class));
    }

    /**
     * Test method for
     * {@link JmxSupport#register(AbstractMetrics, String, String, int)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testRegisterServer() throws JMException {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<Object> captureObject = new Capture<Object>();
        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(
                mockServer.registerMBean(capture(captureObject),
                        capture(captureName))).andReturn(null);

        replay(mockMetrics, mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.register(mockMetrics, "subType", "foo", 1234);

        verify(mockMetrics, mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("serverName"), is("foo"));
        assertThat(name.getKeyProperty("index"), is("1234"));

        final Object bean = captureObject.getValue();
        assertThat(bean, instanceOf(MetricsMXBeanProxy.class));
    }

    /**
     * /** Test method for
     * {@link JmxSupport#register(AbstractMetrics, String, String, int)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testRegisterServerOnJMException() throws JMException {
        final AbstractMetrics mockMetrics = createMock(AbstractMetrics.class);
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<Object> captureObject = new Capture<Object>();
        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(
                mockServer.registerMBean(capture(captureObject),
                        capture(captureName))).andThrow(
                new InstanceAlreadyExistsException());

        replay(mockMetrics, mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.register(mockMetrics, "subType", "foo", 1234);

        verify(mockMetrics, mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("serverName"), is("foo"));
        assertThat(name.getKeyProperty("index"), is("1234"));

        final Object bean = captureObject.getValue();
        assertThat(bean, instanceOf(MetricsMXBeanProxy.class));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregister() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();
        final Capture<ObjectName> captureName2 = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(true);
        mockServer.unregisterMBean(capture(captureName2));
        expectLastCall();

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo");

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("name"), is("foo"));
        assertThat(name.getKeyProperty("index"), nullValue());

        assertThat(captureName2.getValue(), sameInstance(name));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregisterOnJMException() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();
        final Capture<ObjectName> captureName2 = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(true);
        mockServer.unregisterMBean(capture(captureName2));
        expectLastCall().andThrow(new InstanceNotFoundException());

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo");

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("name"), is("foo"));
        assertThat(name.getKeyProperty("index"), nullValue());

        assertThat(captureName2.getValue(), sameInstance(name));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String, int)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregisterServer() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();
        final Capture<ObjectName> captureName2 = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(true);
        mockServer.unregisterMBean(capture(captureName2));
        expectLastCall();

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo", 1234);

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("serverName"), is("foo"));
        assertThat(name.getKeyProperty("index"), is("1234"));

        assertThat(captureName2.getValue(), sameInstance(name));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String, int)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregisterServerOnJMException() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();
        final Capture<ObjectName> captureName2 = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(true);
        mockServer.unregisterMBean(capture(captureName2));
        expectLastCall().andThrow(new InstanceNotFoundException());

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo", 1234);

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("serverName"), is("foo"));
        assertThat(name.getKeyProperty("index"), is("1234"));

        assertThat(captureName2.getValue(), sameInstance(name));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String, int)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregisterServerWhenNotRegistered() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(false);

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo", 1234);

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("serverName"), is("foo"));
        assertThat(name.getKeyProperty("index"), is("1234"));
    }

    /**
     * Test method for {@link JmxSupport#unregister(String, String)}.
     *
     * @throws JMException
     *             On a test failure.
     */
    @Test
    public void testUnregisterWhenNotRegistered() throws JMException {
        final MBeanServer mockServer = createMock(MBeanServer.class);

        final Capture<ObjectName> captureName = new Capture<ObjectName>();

        expect(mockServer.isRegistered(capture(captureName))).andReturn(false);

        replay(mockServer);

        final JmxSupport support = new JmxSupport(mockServer);

        support.unregister("subType", "foo");

        verify(mockServer);

        final ObjectName name = captureName.getValue();
        assertThat(name, notNullValue());
        assertThat(name.getDomain(), is(JmxSupport.DOMAIN_NAME));
        assertThat(name.getKeyProperty("type"), is("metrics"));
        assertThat(name.getKeyProperty("subType"), is("subType"));
        assertThat(name.getKeyProperty("name"), is("foo"));
        assertThat(name.getKeyProperty("index"), nullValue());
    }
}
