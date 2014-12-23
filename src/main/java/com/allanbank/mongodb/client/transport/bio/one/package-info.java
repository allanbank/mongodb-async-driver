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
 * and a single reader thread per socket. Messages are sent using the 
 * application's threads.
 * <p>
 * Since this transport has dedicated read thread all of the 
 * read serialization is done on the read thread and the 
 * {@link com.allanbank.mongodb.client.transport.bio.MessageInputBuffer}
 * and simply contain the message.
 * </p>
 * <p>
 * Conversely, there is no dedicated send thread so the
 * {@link com.allanbank.mongodb.client.transport.bio.one.OneThreadOutputBuffer}
 * serializes the message to an internal buffer that then reduces the send on the 
 * socket (which must be done holding a lock) to a simple write of the 
 * buffered bytes.
 * </p>
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */

package com.allanbank.mongodb.client.transport.bio.one;