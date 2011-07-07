/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

/**
 * Enumeration of the possible operations allowed in MongoDB messages.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum Operation {
	/** Delete documents. */
	DELETE(2006),

	/** Get more data from a query. */
	GET_MORE(2005),

	/** Insert new document. */
	INSERT(2002),

	/** Tell database client is done with a cursor. */
	KILL_CURSORS(2007),

	/** Query a collection. */
	QUERY(2004),

	/** Reply to a client request. */
	REPLY(1),

	/** Update a document. */
	UPDATE(2001);

	/** The operation code. */
	private final int myCode;

	/**
	 * Creates a new Operation.
	 * 
	 * @param code
	 *            The operations code.
	 */
	private Operation(final int code) {
		myCode = code;
	}

	/**
	 * Returns the Operation's code.
	 * 
	 * @return The operation's code.
	 */
	public int getCode() {
		return myCode;
	}
}
