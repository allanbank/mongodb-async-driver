/*
 * #%L
 * Mongo.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface to bootstrap into interactions with MongoDB.
 *
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @deprecated Use the {@link MongoClient} interface instead. This interface
 *             will be removed on or after the 1.3.0 release.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
@ThreadSafe
public interface Mongo
        extends MongoClient {

    /**
     * Returns a Mongo instance that shares connections with this Mongo instance
     * but serializes all of its requests on a single connection.
     * <p>
     * While the returned Mongo instance is thread safe it is intended to be
     * used by a single logical thread to ensure requests issued to the MongoDB
     * server are guaranteed to be processed in the same order they are
     * requested.
     * </p>
     * <p>
     * Creation of the serial instance is lightweight with minimal object
     * allocation and no server interaction.
     * </p>
     *
     * @return Serialized view of the connections to the MongoDB Server.
     * @deprecated Use {@link MongoClient#asSerializedClient()} instead.
     */
    @Deprecated
    public Mongo asSerializedMongo();

    /**
     * Returns the configuration being used by the logical MongoDB connection.
     *
     * @return The configuration being used by the logical MongoDB connection.
     */
    @Override
    public MongoDbConfiguration getConfig();

}
