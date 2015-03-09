/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.bloomgraph.bloom.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.filters.TripleBloomFilter;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

/**
 * Must not used named parameters as they are not supported by all
 * implementations of JDBC
 *
 */
public interface SQLCommands {

	/**
	 * Return the table name of the page index table. primarily used in testing
	 * to truncate the table.
	 * 
	 * @return The table name
	 */
	public String getPageIndexTableName();

	/**
	 * Create a new page and add it to the index. Must add page table, add page
	 * to index and statistics.
	 *
	 * @param stmt
	 *            The statement to use to create the index
	 * @return the number for the new page.
	 * @throws SQLException
	 * @throws IOException
	 */
	public int createPage(final Statement stmt) throws SQLException,
			IOException;

	/**
	 * Create a statement that returns the matching pages. Must return a blob in
	 * the first column and the id in the second.
	 *
	 * @param connection
	 *            The database connection.
	 * @param filter
	 *            The bloom filter to match.
	 * @return The prepared statement that mateches the bloom filter.
	 * @throws SQLException
	 */
	public PreparedStatement pageIndexSearch(Connection connection,
			final PageBloomFilter filter) throws SQLException;

	/**
	 * Return the query that will return the bloom data for a specific page.
	 *
	 * Query must have a replacable parameter for the page number.
	 *
	 * The first column must be the blob that is the page bloom filter.
	 * 
	 * @return The query string.
	 */
	public String pageIndexById();

	// PAGE DATA QUERIES

	/**
	 * Query should perform byte checks. must return data in first and id in
	 * second result columns
	 *
	 * @param connection
	 *            The database connection.
	 * @param pageId
	 *            The page ID of the page to search.
	 * @param bloom
	 *            The bloom filter to match.
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public PreparedStatement tripleSearch(final Connection connection,
			final int pageId, final PageSearchItem pageSearchItem)
			throws SQLException, IOException;

	/**
	 * Return a list of all triples on the page.
	 *
	 * result set must be blob of data in the first column and the triple id
	 * (int) in the second.
	 *
	 * @param connection
	 *            The database connection.
	 * @param pageId
	 *            The page ID of the page to search.
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	public PreparedStatement tripleScan(final Connection connection,
			final int pageId) throws SQLException;

	/**
	 * Delete matching triples from the page.
	 *
	 * @param connection
	 *            The database connection.
	 * @param pageId
	 *            The page ID of the page to search.
	 * @param PageSearchItem
	 *            the page search item describing the triples to delete.
	 * @return number of triples deleted
	 * @throws SQLException
	 * @throws IOException
	 */
	public int tripleDelete(final Connection connection, final DBPage page,
			final PageSearchItem candidate) throws SQLException, IOException;

	/**
	 * Count the matching triples on the page.
	 *
	 * @param connection
	 *            The database connection.
	 * @param pageId
	 *            The page ID of the page to search.
	 * @param PageSearchItem
	 *            the page search item describing the triples to count.
	 * @return the number of matching triples (estimate).
	 * @throws IOException
	 */
	public int tripleCount(final Connection connection, final int pageId,
			final TripleBloomFilter filter) throws SQLException, IOException;

	/**
	 * Insert the triple on the page, update the page index, and the statistics.
	 *
	 * @param connection
	 *            The database connection.
	 * @param pageId
	 *            The page ID of the page to search.
	 * @param PageSearchItem
	 *            the page search item describing the triples to count.
	 * @throws IOException
	 */
	public void tripleInsert(final Connection connection, final int pageId,
			final PageSearchItem candidate) throws SQLException, IOException;

	// PAGE DATA STATISTICS

	public String getPageStatsTableName();

	/**
	 * Define string to create a page data tables. this is actually a format
	 * string that has a %s where the page number should be inserted.
	 *
	 * @return query creates page tables.
	 */
	// public String getCreatePageStats() ;

	public void createSchema(final Connection connection) throws SQLException;

	/**
	 * Returns the number of pages.
	 *
	 * @return query that returns the number of pages.
	 */
	public String getPageCountQuery();

	/**
	 * Define string to create a page data tables. this is actually a format
	 * string that has a %s where the page number should be inserted.
	 *
	 * @return query creates page tables.
	 */
	public String statsIncrementRecord(final int tableId);

	public String statsIncrementDelete(final int tableId);

	public String statsIncrementBytes(final int tableId, final int bytes);

	public String getStats(final int tableId);

	/**
	 * Query should select a page id based on the number of records on the page.
	 * We greatest (records<PAGE_SIZE) SQL = (PAGE_SIZE - records) in case of
	 * tie we want min overs (records-deletes) in case of tie we want min page
	 * id.
	 *
	 * @return a query that returns id, overs, and free
	 */
	public String getBestPageQuery();

	public String getRecordCountQuery();
}
