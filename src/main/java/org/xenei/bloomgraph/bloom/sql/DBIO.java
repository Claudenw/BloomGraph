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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.bloom.BloomIO;
import org.xenei.bloomgraph.bloom.GraphStatistics;
import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.index.PageIndex;
import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * DB implementation of BloomIO.
 *
 */
public class DBIO implements BloomIO {
	private static final Logger LOG = LoggerFactory.getLogger(DBIO.class);

	// set page overs = max page
	public static final int MAX_PAGE_OVERS = PageBloomFilter.CONFIG
			.getNumberOfItems();

	// the datasource for the database.
	private final DataSource dataSource;
	// the statistics
	private final BloomGraphStatistics statistics;
	// the SQL commands for the database implementation.
	private final SQLCommands sqlCommands;
	// the thread local connection.
	private final ThreadLocal<Connection> threadConn;

	/**
	 * Constructor
	 * 
	 * @param dataSource
	 *            The datasource for the connectins.
	 * @param sqlCommands
	 *            The SQL commands for the database implementation.
	 * @throws SQLException
	 *             on error.
	 */
	public DBIO(final DataSource dataSource, final SQLCommands sqlCommands)
			throws SQLException {
		this.dataSource = dataSource;
		this.statistics = new BloomGraphStatistics();
		this.sqlCommands = sqlCommands;
		this.threadConn = new ThreadLocal<Connection>();
		createSchema();
	}

	/**
	 * Create the schema if necessary. This method must check the schema to
	 * ensure that it is in a known state.
	 * 
	 * @throws SQLException
	 *             on error.
	 */
	private void createSchema() throws SQLException {
		sqlCommands.createSchema(getConnection());
	}

	@Override
	public GraphStatistics getStatistics() {
		return statistics;
	}

	/**
	 * Get the connection. The DBIO will create a a single connection on the
	 * first call and will store it on the thread local varialble. After first
	 * call the thread local version is returned.
	 * 
	 * @return the connection.
	 * @throws SQLException
	 *             on error.
	 */
	public Connection getConnection() throws SQLException {
		Connection retval = threadConn.get();
		if (retval == null) {
			retval = dataSource.getConnection();
			threadConn.set(retval);
		}
		return retval;
	}

	/**
	 * The the commands for the DB instance.
	 * 
	 * @return the SQL commands.
	 */
	public SQLCommands getSqlCommands() {
		return sqlCommands;
	}

	@Override
	public final ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException {
		final Iterator<Iterator<Triple>> inner = WrappedIterator.create(
				new PageIndexIterator(candidate.getPageFilter())).mapWith(
				new Map1<SQLPageIndex, Iterator<Triple>>() {
					@Override
					public Iterator<Triple> map1(final SQLPageIndex pageIndex) {
						try {
							return getPage(pageIndex).find(candidate);
						} catch (final IOException e) {
							throw new IllegalStateException(e.getMessage(), e);
						} catch (final SQLException e) {
							throw new IllegalStateException(e.getMessage(), e);
						}
					}
				});
		return WrappedIterator.create(new IterIter(inner));
	}

	@Override
	public long count(final PageSearchItem candidate) throws IOException {

		final ClosableIterator<SQLPageIndex> iter = new PageIndexIterator(
				candidate.getPageFilter());
		long retval = 0;
		try {
			while (iter.hasNext()) {
				try {
					retval += getPage(iter.next()).count(candidate);
				} catch (final SQLException e) {
					LOG.warn(e.getMessage(), e);
				}
			}
			return retval;
		} finally {
			iter.close();
		}
	}

	@Override
	public AbstractPage getPage(final int idx) throws IOException {
		try {
			return getPage(getPageIndex(idx));
		} catch (final SQLException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	@Override
	public int getPageCount() throws IOException {
		return statistics.pages();
	}

	@Override
	public int getPageIndexOrigin() {
		return 1;
	}

	/**
	 * Get the page specified by the index.
	 * 
	 * @param pageIndex
	 *            The index to find.
	 * @return The Page.
	 * @throws SQLException
	 */
	private AbstractPage getPage(final PageIndex pageIndex) throws SQLException {
		return new DBPage(pageIndex, this);
	}

	/**
	 * Get the page index for the specified page.
	 * 
	 * @param idx
	 *            the page id to retrieve the index for.
	 * @return The requiested PageIndex
	 * @throws SQLException
	 *             on SQL error
	 * @throws IOException
	 *             on IO error
	 */
	public PageIndex getPageIndex(final int idx) throws SQLException,
			IOException {
		final PreparedStatement stmt = getConnection().prepareStatement(
				sqlCommands.pageIndexById());
		ResultSet rs = null;
		Blob blob = null;
		try {
			stmt.setInt(1, idx);
			rs = stmt.executeQuery();
			if (!rs.next()) {
				return new PageIndex(idx);
			}
			blob = rs.getBlob(1);
			if (blob == null) {
				return new PageIndex(idx);
			}
			return new SQLPageIndex(blob, idx);
		} finally {
			DBIO.freeQuietly(blob);
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public final void add(final PageSearchItem candidate) throws IOException {
		// locate a page by querying for page that has the least space
		// lower than max inserts
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			int id = 0;
			stmt = getConnection().prepareStatement(
					sqlCommands.getBestPageQuery());
			rs = stmt.executeQuery();
			if (!rs.next()) {
				id = createPage();
			}
			else {
				if (rs.getInt(2) >= MAX_PAGE_OVERS) {
					LOG.warn(
							"Maximum Overs reached on page {} -- cleaning required.",
							rs.getInt(1));
					id = createPage();
				}
				else {
					id = rs.getInt(1);
				}
			}
			getPage(id).write(candidate);
		} catch (final SQLException e) {
			throw new IOException(e.getMessage(), e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	/**
	 * Create a new page.
	 * 
	 * @return the page id.
	 * @throws SQLException
	 * @throws IOException
	 */
	private int createPage() throws SQLException, IOException {
		Statement stmt = null;
		try {
			stmt = getConnection().createStatement();
			return sqlCommands.createPage(stmt);
		} finally {
			DbUtils.closeQuietly(stmt);
		}

	}

	private void lock() {
	}

	private void unlock() {
	};

	private void flush() {
	};

	@Override
	public final void delete(final PageSearchItem candidate) throws IOException {

		final ClosableIterator<SQLPageIndex> iter = new PageIndexIterator(
				candidate.getPageFilter());
		try {
			while (iter.hasNext()) {
				try {
					getPage(iter.next()).delete(candidate);
				} catch (final SQLException e) {
					throw new IOException(e.getMessage(), e);
				}
			}
		} finally {
			iter.close();
		}
	}

	/**
	 * copy the blob to a byte buffer.
	 * 
	 * @param blob
	 *            The blob to read
	 * @return A bytebuffer with the blob contents.
	 * @throws IOException
	 *             on copy error
	 * @throws SQLException
	 *             on db error
	 */
	public static ByteBuffer toByteBuffer(final Blob blob) throws IOException,
			SQLException {
		ByteArrayOutputStream baos = null;
		try {
			baos = new ByteArrayOutputStream();
			IOUtils.copy(blob.getBinaryStream(), baos);
			return ByteBuffer.wrap(baos.toByteArray()).order(
					ByteOrder.LITTLE_ENDIAN);
		} finally {
			IOUtils.closeQuietly(baos);
		}
	}

	/**
	 * Convert the byte buffer to a input stream.
	 * 
	 * @param buffer
	 *            the buffer to conver
	 * @return the input stream.
	 */
	public static InputStream asInputStream(final ByteBuffer buffer) {
		final ByteBuffer buff = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
		if (buff.hasArray()) {
			// use heap buffer; no array is created; only the reference is used
			return new ByteArrayInputStream(buff.array());
		}
		return new ByteBufferInputStream(buff);
	}

	/**
	 * free the blob ignoring any errors.
	 * 
	 * @param blob
	 *            The blob to free.
	 */
	public static void freeQuietly(final Blob blob) {
		if (blob != null) {
			try {
				blob.free();
			} catch (final SQLException e) {
				LOG.warn("Ignoring error freeing blob: " + e.getMessage(), e);
			}
		}
	}

	/**
	 * a SQL based PageIndex.
	 *
	 */
	private class SQLPageIndex extends PageIndex {
		private SQLPageIndex(final Blob blob, final int id) throws IOException,
				SQLException {
			super(toByteBuffer(blob), id);
		}
	}

	/**
	 * Iterator over all PageIndex
	 *
	 */
	private class PageIndexIterator implements ClosableIterator<SQLPageIndex> {
		private PreparedStatement stmt;
		private ResultSet rs;
		private SQLPageIndex next;
		private final PageBloomFilter filter;

		/**
		 * Constructor
		 * 
		 * @param filter
		 *            The filter to iterate by.
		 */
		private PageIndexIterator(final PageBloomFilter filter) {
			this.stmt = null;
			this.rs = null;
			this.next = null;
			this.filter = filter;
		}

		/**
		 * make sure we close the DB objects.
		 */
		@Override
		public void finalize() {
			close();
		}

		@Override
		public void close() {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}

		@Override
		public final boolean hasNext() {
			if (next == null) {
				try {
					if (rs == null) {
						if (stmt == null) {
							stmt = sqlCommands.pageIndexSearch(getConnection(),
									filter);
						}
						rs = stmt.executeQuery();
					}
					if (rs.isClosed()) {
						return false;
					}

					if (!rs.isAfterLast()) {
						if (rs.next()) {
							Blob blob = null;
							try {
								blob = rs.getBlob(1);
								next = new SQLPageIndex(blob, rs.getInt(2));
							} finally {
								DBIO.freeQuietly(blob);
							}
						}
						else {
							close();
						}
					}
					else {
						DbUtils.closeQuietly(rs);
					}

				} catch (final SQLException e) {
					LOG.error(e.getMessage(), e);
					close();
				} catch (final IOException e) {
					LOG.error(e.getMessage(), e);
					close();
				}
			}
			return next != null;
		}

		@Override
		public final SQLPageIndex next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}

	}

	/**
	 * An inputstream based on a byte buffer.
	 *
	 */
	private static class ByteBufferInputStream extends InputStream {

		private final ByteBuffer buf;

		/**
		 * Constructor.
		 * 
		 * @param buf
		 *            The buffer to read.
		 */
		private ByteBufferInputStream(final ByteBuffer buf) {
			this.buf = buf;
		}

		@Override
		public int read() throws IOException {
			if (!buf.hasRemaining()) {
				return -1;
			}
			return buf.get() & 0xFF;
		}

		@Override
		public int read(final byte[] bytes, final int off, int len)
				throws IOException {
			if (!buf.hasRemaining()) {
				return -1;
			}

			len = Math.min(len, buf.remaining());
			buf.get(bytes, off, len);
			return len;
		}
	}

	/**
	 * Graph statistics.
	 *
	 */
	private class BloomGraphStatistics implements GraphStatistics {

		@Override
		public long getStatistic(final Node S, final Node P, final Node O) {

			try {
				return count(new PageSearchItem(new Triple(S, P, O)));
			} catch (final IOException e) {
				LOG.warn(e.getMessage(), e);
				return -1;
			}
		}

		@Override
		public long size() {
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = getConnection().createStatement();
				rs = stmt.executeQuery(sqlCommands.getRecordCountQuery());
				if (rs.next()) {
					return rs.getInt(1);
				}
				else {
					return -1;
				}
			} catch (final SQLException e) {
				LOG.error(e.getMessage(), e);
				return -1;
			} finally {
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(stmt);
			}
		}

		@Override
		public int pages() {
			Statement stmt = null;
			ResultSet rs = null;
			try {
				stmt = getConnection().createStatement();
				rs = stmt.executeQuery(sqlCommands.getPageCountQuery());
				if (rs.next()) {
					return rs.getInt(1);
				}
				else {
					return -1;
				}
			} catch (final SQLException e) {
				LOG.error(e.getMessage(), e);
				return -1;
			} finally {
				DbUtils.closeQuietly(rs);
				DbUtils.closeQuietly(stmt);
			}
		}

	}
}
