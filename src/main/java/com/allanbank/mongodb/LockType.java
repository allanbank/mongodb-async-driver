/*
 * #%L
 * LockType.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * LockType provides an enumeration for the types of locks used at the core of
 * the driver to hand off messages between threads.
 * 
 * @api.yes This interface is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum LockType {

    /**
     * Use a spin loop. This produces an extremely low latency hand-off at the
     * expense of utilizing more CPU than a standard mutex. If the hand-off does
     * not occur within 1 ms then this lock falls back to a mutex.
     * <p>
     * Generally only applications looking for the absolute best performance and
     * that CPU capacity will need this type of locking.
     * </p>
     */
    LOW_LATENCY_SPIN,

    /** Use a standard Java mutex for notification across threads. */
    MUTEX;
}
