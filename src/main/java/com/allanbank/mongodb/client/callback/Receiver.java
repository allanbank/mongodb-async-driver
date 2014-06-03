/*
 * #%L
 * Receiver.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.callback;

/**
 * Receiver provides an interface for a class that receives messages from the
 * server so that blocking futures can try and periodically receive messages.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Receiver {
    /**
     * Tries to receive a message from the server. Every attempt should be made
     * to not block.
     */
    public void tryReceive();
}
