/*
 * #%L
 * package-info.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * The two thread transport implementation that uses blocking I/O sockets 
 * and a single reader thread and a single writer thread per socket.
 * <p>
 * Since this transport has dedicated read and write threads all of the 
 * serialization is done on the read and write threads and the 
 * {@link com.allanbank.mongodb.client.transport.bio.MessageInputBuffer}
 * and {@link com.allanbank.mongodb.client.transport.bio.two.TwoThreadOutputBuffer}
 * both simply contain the message.
 * </p>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.client.transport.bio.two;

