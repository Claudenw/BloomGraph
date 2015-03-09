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
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.SerializableTriple;
import org.xenei.bloomgraph.bloom.index.PageIndex;
import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;
import org.xenei.bloomgraph.bloom.page.SerializableTripleFilter;
import org.xenei.bloomgraph.bloom.page.UpdatablePageStatistics;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * DB implementation of AbstractPage.
 *
 */
public class DBPage extends AbstractPage {
	private final Logger log;
	// updatable page statistics
	private final UpdatablePageStatistics statistics;
	// the DB IO implementatin.
	private final DBIO io;

	/**
	 * Constructor.
	 * 
	 * @param pageIndex
	 *            The page index for htis page
	 * @param io
	 *            the DBIO implementation for this page.
	 */
	public DBPage(final PageIndex pageIndex, final DBIO io) {
		super(pageIndex);
		this.log = LoggerFactory.getLogger(String.format("%s.%s",
				DBPage.class.getName(), pageIndex.getId()));
		this.io = io;
		this.statistics = new Statistics();
	}

	@Override
	protected void lock() {
		debug("Lock");
	};

	@Override
	protected void unlock() {
		debug("Unlock");
	};

	@Override
	protected void flush() {
		debug("Flush");
	};

	@Override
	public UpdatablePageStatistics getUpdatableStatistics() {
		return statistics;
	}

	@Override
	public ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException {

		return WrappedIterator
				.create(new SerializableTripleIterator(candidate))
				.filterKeep(new SerializableTripleFilter(candidate))
				.mapWith(new Map1<SerializableTriple, Triple>() {

					@Override
					public Triple map1(final SerializableTriple o) {
						try {
							return o.getTriple();
						} catch (final IOException e) {
							throw new IllegalArgumentException(e.getMessage(),
									e);
						}
					}
				});
	}

	@Override
	public int doCount(final PageSearchItem candidate) throws IOException {
		try {
			return io.getSqlCommands().tripleCount(io.getConnection(),
					pageIndex.getId(), candidate.getTripleFilter());
		} catch (final SQLException e) {
			log.error(e.getMessage());
			throw new IOException(e);
		}
	}

	@Override
	public boolean doWrite(final PageSearchItem candidate) throws IOException {

		// make sure the objects have been created
		candidate.getSerializable();
		candidate.getTripleFilter();
		log.debug("writing {}", candidate.getTripleFilter());

		lock();
		try {
			// write the record to the page
			io.getSqlCommands().tripleInsert(io.getConnection(),
					pageIndex.getId(), candidate);
			flush();
			return true;
		} catch (final SQLException e) {
			log.error(e.getMessage(), e);
			throw new IOException(e);
		} finally {
			unlock();
		}
	}

	@Override
	public int delete(final PageSearchItem candidate) throws IOException {
		int count = 0;
		final ExtendedIterator<SerializableTriple> iter = null;
		lock();
		try {
			count = io.getSqlCommands().tripleDelete(io.getConnection(), this,
					candidate);
			flush();
			return count;
		} catch (final SQLException e) {
			throw new IOException(e.getMessage(), e);
		} finally {
			unlock();
		}
	}

	/**
	 * DB implementation fo UpdatablePageStatistics
	 *
	 */
	private class Statistics extends UpdatablePageStatistics {
		private int records;
		private int deletes;
		private int dataSize;
		// the timestamp for the last update.
		private final long lastUpdate;
		// how old the statistics can be and still be considered current.
		private static final long maxAge = 60 * 1000; // 60 seconds

		/**
		 * Constructor
		 */
		private Statistics() {
			// no statistics loaded yet.
			lastUpdate = 0;
		}

		/**
		 * See if we need to update the statistics before returning a value.
		 */
		private void checkUpdate() {
			if (lastUpdate < (System.currentTimeMillis() - maxAge)) {
				Statement stmt = null;
				ResultSet rs = null;
				try {
					stmt = io.getConnection().createStatement();
					rs = stmt.executeQuery(io.getSqlCommands().getStats(
							pageIndex.getId()));
					if (rs.next()) {
						records = rs.getInt(1);
						deletes = rs.getInt(2);
						dataSize = rs.getInt(3);
					}
					else {
						log.error("Unable to retrieve statistics for page "
								+ pageIndex.getId());
					}
				} catch (final SQLException e) {
					log.error("Error retrieving statistics for page "
							+ pageIndex.getId(), e);
				} finally {
					DbUtils.closeQuietly(rs);
					DbUtils.closeQuietly(stmt);
				}
			}
		}

		@Override
		public int getRecordCount() {
			checkUpdate();
			return records;
		}

		@Override
		public void incrementRecordCount() {
			Statement stmt = null;
			try {
				stmt = io.getConnection().createStatement();
				stmt.executeUpdate(io.getSqlCommands().statsIncrementRecord(
						pageIndex.getId()));
			} catch (final SQLException e) {
				log.error(
						"Error updating record count for page "
								+ pageIndex.getId(), e);
			} finally {
				DbUtils.closeQuietly(stmt);
			}
			records++;
		}

		@Override
		public int getDataSize() {
			checkUpdate();
			return dataSize;
		}

		@Override
		public void incrementDataSize(final int size) {
			Statement stmt = null;
			try {
				stmt = io.getConnection().createStatement();
				stmt.executeUpdate(io.getSqlCommands().statsIncrementBytes(
						pageIndex.getId(), size));
			} catch (final SQLException e) {
				log.error(
						"Error updating data size for page "
								+ pageIndex.getId(), e);
			} finally {
				DbUtils.closeQuietly(stmt);
			}
			dataSize += size;
		}

		@Override
		public int getDeleteCount() {
			checkUpdate();
			return deletes;
		}

		@Override
		public void incrementDeleteCount() {
			Statement stmt = null;
			try {
				stmt = io.getConnection().createStatement();
				stmt.executeUpdate(io.getSqlCommands().statsIncrementDelete(
						pageIndex.getId()));
			} catch (final SQLException e) {
				log.error(
						"Error updating delets count for page "
								+ pageIndex.getId(), e);
			} finally {
				DbUtils.closeQuietly(stmt);
			}
			deletes++;
		}

	}

	/**
	 * Iterate over the triples in the page that match the candidate bloom
	 * filter.
	 *
	 */
	public class SerializableTripleIterator implements
			ClosableIterator<SerializableTriple> {
		private PreparedStatement stmt;
		private ResultSet rs;
		private SerializableTriple next;
		private PageSearchItem candidate;

		/**
		 * Constructor.
		 * 
		 * @param candidate
		 *            the page search item to match.
		 */
		public SerializableTripleIterator(final PageSearchItem candidate) {
			this.stmt = null;
			this.rs = null;
			this.next = null;
			if (candidate.getTriple().equals(Triple.ANY)) {
				this.candidate = null;
			}
			else {
				this.candidate = candidate;
			}
		}

		/**
		 * Make sure we close the DB resources.
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

		/**
		 * Read a serializable triple from the database.
		 * 
		 * @param rs
		 *            The result set to read the serializable triple from.
		 * @return the SerializableTriple.
		 * @throws IOException
		 * @throws SQLException
		 */
		private SerializableTriple readResult(final ResultSet rs)
				throws IOException, SQLException {
			Blob blob = null;
			try {
				blob = rs.getBlob(1);
				final SerializableTriple retval = new SerializableTriple(
						DBIO.toByteBuffer(blob));
				retval.setIndex(rs.getInt(2));
				return retval;
			} finally {
				DBIO.freeQuietly(blob);
			}
		}

		@Override
		public final boolean hasNext() {
			if (next == null) {
				try {
					if (rs == null) {
						if (stmt == null) {
							if (candidate != null) {
								stmt = io.getSqlCommands().tripleSearch(
										io.getConnection(), pageIndex.getId(),
										candidate);

							}
							else {
								stmt = io.getSqlCommands().tripleScan(
										io.getConnection(), pageIndex.getId());
							}
						}
						rs = stmt.executeQuery();
						if (rs.next()) {
							next = readResult(rs);
						}
					}
					if (rs.isClosed()) {
						return false;
					}
					if (next == null) {
						if (!rs.isAfterLast()) {
							if (rs.next()) {
								next = readResult(rs);
							}
							else {
								close();
							}
						}
					}
				} catch (final SQLException e) {
					log.error(e.getMessage(), e);
					close();
				} catch (final IOException e) {
					log.error(e.getMessage(), e);
					close();
				}
			}
			return next != null;
		}

		@Override
		public final SerializableTriple next() {
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
}
