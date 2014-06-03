/*
 * #%L
 * MongoClientConfigurationBeanInfo.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditorSupport;
import java.beans.SimpleBeanInfo;
import java.util.Collections;

import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * MongoClientConfigurationBeanInfo provides specialization for the properties
 * of the {@link MongoClientConfiguration}.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientConfigurationBeanInfo extends SimpleBeanInfo {

    /**
     * Creates a new MongoClientConfigurationBeanInfo.
     */
    public MongoClientConfigurationBeanInfo() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return <code>null</code> to trigger normal bean
     * introspection.
     * </p>
     */
    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {

        try {
            final BeanInfo beanInfo = Introspector.getBeanInfo(
                    MongoClientConfiguration.class,
                    Introspector.IGNORE_IMMEDIATE_BEANINFO);
            final PropertyDescriptor[] descriptors = beanInfo
                    .getPropertyDescriptors();

            for (final PropertyDescriptor descriptor : descriptors) {
                if ("credentials".equalsIgnoreCase(descriptor.getName())) {
                    descriptor
                            .setPropertyEditorClass(CredentialListEditor.class);
                }
            }

            return descriptors;
        }
        catch (final IntrospectionException e) {
            // Just use the defaults.
            return super.getPropertyDescriptors();
        }
    }

    /**
     * CredentialListEditor provides the ability to parse the list of
     * credentials from a MongoDB URI. This will always be a singleton list.
     * 
     * @api.no This class is <b>NOT</b> part of the drivers API. This class may
     *         be mutated in incompatible ways between any two releases of the
     *         driver.
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static class CredentialListEditor extends PropertyEditorSupport {

        /** The logger for the {@link CredentialListEditor}. */
        protected static final Log LOG = LogFactory
                .getLog(CredentialListEditor.class);

        /**
         * Creates a new CredentialEditor.
         */
        public CredentialListEditor() {
            super();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to parse a string to a {@link Credential}.
         * </p>
         * 
         * @throws IllegalArgumentException
         *             If the string cannot be parsed into a {@link Credential}.
         */
        @Override
        public void setAsText(final String credentialString)
                throws IllegalArgumentException {

            final CredentialEditor realEditor = new CredentialEditor();
            realEditor.setAsText(credentialString);
            final Object value = realEditor.getValue();

            if (value != null) {
                setValue(Collections.singletonList(value));
            }
            else {
                setValue(Collections.EMPTY_LIST);
            }
        }
    }
}
