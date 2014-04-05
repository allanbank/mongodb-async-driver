/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
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
