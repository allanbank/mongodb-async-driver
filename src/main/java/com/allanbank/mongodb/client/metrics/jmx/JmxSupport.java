/*
 * #%L
 * JmxSupport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.Hashtable;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.client.metrics.AbstractMetrics;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * JmxSupport provides helper methods for adding and removing MBeans to the
 * {@link MBeanServer}.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class JmxSupport {

    /** The domain name for registering MBeans. */
    protected static final String DOMAIN_NAME = MongoClient.class.getPackage()
            .getName();

    /** The logger for the {@link JmxSupport}. */
    private static final Log LOG = LogFactory.getLog(JmxSupport.class);

    /**
     * The size of the random value in bytes pre-Base64 encoding:
     * {@value #RANDOM_BYTES}. Chosen to have no Base64 padding.
     */
    private static final int RANDOM_BYTES = 6;

    /** The MBeanServer for the platform. */
    private final MBeanServer myServer;

    /**
     * A random value use with all of the client's MBeans to make sure they are
     * unique.
     */
    private final String myUniqueId;

    /**
     * Creates a new JmxSupport.
     * 
     * @param server
     *            The MBeanServer for the platform.
     */
    public JmxSupport(final MBeanServer server) {
        myServer = server;

        final SecureRandom random = new SecureRandom();
        final byte[] bytes = new byte[RANDOM_BYTES];
        random.nextBytes(bytes);
        myUniqueId = IOUtils.toBase64(bytes);
    }

    /**
     * Registers the metrics with the MBeanServer under the specified sub-type,
     * and name.
     * 
     * @param metrics
     *            The metrics to register.
     * @param subType
     *            The sub type for the MBean.
     * @param name
     *            The name of the MBean.
     */
    public void register(final AbstractMetrics metrics, final String subType,
            final String name) {
        try {
            final ObjectName objectName = createName(subType, name);

            myServer.registerMBean(new MetricsMXBeanProxy(metrics), objectName);
        }
        catch (final JMException e) {
            LOG.warn("Failure registering MBean subType={}, name={}: {}",
                    subType, name, e.getMessage());
        }
        catch (final UnsupportedEncodingException e) {
            LOG.warn("Failure encoding MBean name subType={}, name={}: {}",
                    subType, name, e.getMessage());
        }
    }

    /**
     * Registers the metrics with the MBeanServer under the specified sub-type,
     * server name and index.
     * 
     * @param metrics
     *            The metrics to register.
     * @param subType
     *            The sub type for the MBean.
     * @param serverName
     *            The serverName of the MBean.
     * @param index
     *            The index of the MBean.
     */
    public void register(final AbstractMetrics metrics, final String subType,
            final String serverName, final int index) {
        try {
            final ObjectName objectName = createName(subType, serverName, index);

            myServer.registerMBean(new MetricsMXBeanProxy(metrics), objectName);
        }
        catch (final JMException e) {
            LOG.warn(
                    "Failure registering MBean subType={}, serverName={}, index={}: {}",
                    subType, serverName, Integer.valueOf(index), e.getMessage());
        }
        catch (final UnsupportedEncodingException e) {
            LOG.warn(
                    "Failure encoding MBean name subType={}, serverName={}, index={}: {}",
                    subType, serverName, Integer.valueOf(index), e.getMessage());
        }
    }

    /**
     * Unregisters the MBean with the specified sub-type and name.
     * 
     * @param subType
     *            The sub type for the MBean.
     * @param name
     *            The name of the MBean.
     */
    public void unregister(final String subType, final String name) {
        try {
            final ObjectName objectName = createName(subType, name);

            if (myServer.isRegistered(objectName)) {
                myServer.unregisterMBean(objectName);
            }
        }
        catch (final JMException e) {
            LOG.warn("Failure unregistering MBean subType={}, name={}: {}",
                    subType, name, e.getMessage());
        }
        catch (final UnsupportedEncodingException e) {
            LOG.warn("Failure encoding MBean name subType={}, name={}: {}",
                    subType, name, e.getMessage());
        }
    }

    /**
     * Unregisters the MBean with the specified sub-type, server name and index.
     * 
     * @param subType
     *            The sub type for the MBean.
     * @param serverName
     *            The serverName of the MBean.
     * @param index
     *            The index of the MBean.
     */
    public void unregister(final String subType, final String serverName,
            final int index) {
        try {
            final ObjectName objectName = createName(subType, serverName, index);

            if (myServer.isRegistered(objectName)) {
                myServer.unregisterMBean(objectName);
            }
        }
        catch (final JMException e) {
            LOG.warn(
                    "Failure unregistering MBean subType={}, serverName={}, index={}: {}",
                    subType, serverName, Integer.valueOf(index), e.getMessage());
        }
        catch (final UnsupportedEncodingException e) {
            LOG.warn(
                    "Failure encoding MBean name subType={}, serverName={}, index={}: {}",
                    subType, serverName, Integer.valueOf(index), e.getMessage());
        }
    }

    /**
     * Creates the object name for the specified sub-type and name.
     * 
     * @param subType
     *            The sub type for the MBean.
     * @param name
     *            The name of the MBean.
     * @return The {@link ObjectName} for the specified sub-type and name.
     * @throws UnsupportedEncodingException
     *             On a failure to encode the object name.
     * @throws JMException
     *             On a failure to create the object name.
     */
    protected ObjectName createName(final String subType, final String name)
            throws UnsupportedEncodingException, JMException {
        final Hashtable<String, String> attrs = new Hashtable<String, String>();
        attrs.put("type", "metrics");
        attrs.put("subType", URLEncoder.encode(subType, "UTF-8"));
        attrs.put("name", URLEncoder.encode(name, "UTF-8"));
        attrs.put("id", myUniqueId);
        return new ObjectName(DOMAIN_NAME, attrs);
    }

    /**
     * Creates the object name for the specified sub-type, server name and
     * index.
     * 
     * @param subType
     *            The sub type for the MBean.
     * @param serverName
     *            The serverName of the MBean.
     * @param index
     *            The index of the MBean.
     * @return The {@link ObjectName} for the specified sub-type and name.
     * @throws UnsupportedEncodingException
     *             On a failure to encode the object name.
     * @throws JMException
     *             On a failure to create the object name.
     */
    protected ObjectName createName(final String subType,
            final String serverName, final int index)
            throws UnsupportedEncodingException, JMException {

        final Hashtable<String, String> attrs = new Hashtable<String, String>();
        attrs.put("type", "metrics");
        attrs.put("subType", URLEncoder.encode(subType, "UTF-8"));
        attrs.put("serverName", URLEncoder.encode(serverName, "UTF-8"));
        attrs.put("index", String.valueOf(index));
        attrs.put("id", myUniqueId);

        return new ObjectName(DOMAIN_NAME, attrs);
    }
}
