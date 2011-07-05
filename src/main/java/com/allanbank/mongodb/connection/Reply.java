/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.bson.Document;

/**
 * Provides the contents of the reply message.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class Reply {
	/** Bit for the await capable flag. */
	public static final int AWAIT_CAPABLE_BIT = 8;

	/** Bit for the cursor not found flag. */
	public static final int CURSOR_NOT_FOUND_BIT = 1;

	/** Bit for the query failure flag. */
	public static final int QUERY_FAILURE_BIT = 2;

	/** Bit for the shard configuration stale flag. */
	public static final int SHARD_CONFIG_STALE_BIT = 4;

	/** Indicates the server is await capable for tailable cursors. */
	private final boolean myAwaitCapable;

	/** Indicates that the cursor in the <tt>getmore</tt> command was not found. */
	private final boolean myCursorNotFound;

	/** The id of the request this response is for. */
	private final int myId;

	/** Indicates that the query failed. */
	private final boolean myQueryFailed;

	/** The returned documents. */
	private final List<Document> myResults;

	/** Indicates (to a MongoS?) that its shard configuration is stale. */
	private final boolean myShardConfigStale;

	/**
	 * The index of the first document in the result in the overall cursor's
	 * results.
	 */
	private final int myStartingIndex;

	/**
	 * Creates a new Reply.
	 * 
	 * @param id
	 *            The id of the request this response is for.
	 * @param flags
	 *            The bit flags from the results.
	 * @param cursorId
	 *            The cursor id returned. If zero then this is the end of the
	 *            results.
	 * @param startingIndex
	 *            The index of the first document in the result in the overall
	 *            cursor's results.
	 * @param results
	 *            The returned documents.
	 */
	public Reply(final int id, final int flags, final long cursorId,
			final int startingIndex, final List<Document> results) {
		myCursorNotFound = ((flags & CURSOR_NOT_FOUND_BIT) == CURSOR_NOT_FOUND_BIT);
		myQueryFailed = ((flags & QUERY_FAILURE_BIT) == QUERY_FAILURE_BIT);
		myShardConfigStale = ((flags & SHARD_CONFIG_STALE_BIT) == SHARD_CONFIG_STALE_BIT);
		myAwaitCapable = ((flags & AWAIT_CAPABLE_BIT) == AWAIT_CAPABLE_BIT);

		myId = id;
		myStartingIndex = startingIndex;
		myResults = Collections.unmodifiableList(new ArrayList<Document>(
				results));
	}

	/**
	 * Returns the id of the request this response is for.
	 * 
	 * @return The id of the request this response is for.
	 */
	public int getId() {
		return myId;
	}

	/**
	 * Returns the returned documents.
	 * 
	 * @return The returned documents.
	 */
	public List<Document> getResults() {
		return myResults;
	}

	/**
	 * Returns the index of the first document in the result in the overall
	 * cursor's results.
	 * 
	 * @return The index of the first document in the result in the overall
	 *         cursor's results.
	 */
	public int getStartingIndex() {
		return myStartingIndex;
	}

	/**
	 * Returns true if the server is await capable, false otherwise.
	 * 
	 * @return True if the server is await capable, false otherwise.
	 */
	public boolean isAwaitCapable() {
		return myAwaitCapable;
	}

	/**
	 * Returns true if the cursor was not found, false otherwise.
	 * 
	 * @return True if the cursor was not found, false otherwise.
	 */
	public boolean isCursorNotFound() {
		return myCursorNotFound;
	}

	/**
	 * Returns true if the query failed, false otherwise.
	 * 
	 * @return True if the query failed, false otherwise.
	 */
	public boolean isQueryFailed() {
		return myQueryFailed;
	}

	/**
	 * Returns true if the shard configuration is stale, false otherwise.
	 * 
	 * @return Returns true if the shard configuration is stale, false
	 *         otherwise.
	 */
	public boolean isShardConfigStale() {
		return myShardConfigStale;
	}
}
