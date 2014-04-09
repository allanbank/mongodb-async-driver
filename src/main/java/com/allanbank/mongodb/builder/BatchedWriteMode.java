/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.builder.write.WriteOperationType;

/**
 * BatchedWriteMode provides enumeration of the available modes for submitting
 * the batched writes.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum BatchedWriteMode {
    /**
     * Indicates that the writes can be reordered to optimize the write
     * performance. This included re-ordering writes within a
     * {@link WriteOperationType} (e.g., move one insert before another to
     * better pack messages to the server) and across types (e.g., move an
     * insert before an update to group all like operations).
     * <p>
     * This mode should provide close to optimal performance.
     * </p>
     */
    REORDERED,

    /**
     * Indicates that the writes should be submitted to the server in the order
     * they were added to the {@link BatchedWrite} but that all writes should be
     * tried on the server even after a failure.
     * <p>
     * This mode will perform better than the {@link #SERIALIZE_AND_STOP} mode
     * since the driver can submit multiple writes to the server at once.
     * </p>
     */
    SERIALIZE_AND_CONTINUE,

    /**
     * Indicates that the writes should be submitted to the server in the order
     * they were added to the {@link BatchedWrite} and that the writes should
     * stop on the first write failure.
     * <p>
     * This mode will perform slower than all of the other modes since the
     * driver must wait for the results of one write before submitting the next.
     * </p>
     */
    SERIALIZE_AND_STOP;
}