/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

/**
 * AddressAware provides an interface for callbacks that need to know the server
 * that was sent the request.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface AddressAware {

    /**
     * Sets the address sent the request.
     * 
     * @param address
     *            The address of the server sent the request.
     */
    public void setAddress(String address);
}
