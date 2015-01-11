/*
 * #%L
 * ServerLatencyComparator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.state;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares {@link Server}'s based on the latency of the servers.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyComparator
        implements Comparator<Server>, Serializable {

    /** A singleton instance of the comparator. No need to multiple instances. */
    public static final Comparator<Server> COMPARATOR = new ServerLatencyComparator();

    /** Serialization version of the class. */
    private static final long serialVersionUID = -7926757327660948536L;

    /**
     * Creates a new {@link ServerLatencyComparator}.
     */
    private ServerLatencyComparator() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Compares the servers based on their respective average latencies.
     * </p>
     *
     * @see Comparator#compare
     */
    @Override
    public int compare(final Server o1, final Server o2) {
        return Double.compare(o1.getAverageLatency(), o2.getAverageLatency());
    }

}
