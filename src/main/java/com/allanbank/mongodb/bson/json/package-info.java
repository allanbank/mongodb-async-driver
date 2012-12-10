/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

/**
 * Contains a parser of JSON documents based on a JavaCC grammer.  The intent of this
 * parser is not to create a fully validating JSON/ECMAScript parser but instead to provide 
 * a simplified parser that accepts most valid documents and output BSON Documents.  The 
 * parsers may also accept invalid documents.
 * 
 * @see <a href="http://java.net/projects/javacc">JavaCC</a>
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.mongodb.bson.json;