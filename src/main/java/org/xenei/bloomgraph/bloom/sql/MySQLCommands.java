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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.SerializableTriple;
import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.filters.TripleBloomFilter;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;
import org.xenei.bloomgraph.bloom.page.SerializableTripleFilter;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * MySQL implementation of SQL commands
 *
 */
public class MySQLCommands implements SQLCommands {

	/**
	 * Get the blob type based on the number of bytes required.
	 * 
	 * @param bytes
	 *            the number of bytes required for object.
	 * @return The string representing the MySQL database type.
	 */
	private static String getBlobType(final int bytes) {
		// 2^8
		if (bytes < 256) {
			return String.format("VARBINARY(%s)", bytes);
		}
		// 2^15
		if (bytes < 65536) {
			return "BLOB";
		}
		// 2^24
		if (bytes < 16777216) {
			return "MEDIUMBLOB";
		}
		// int is always less than 2^32 (max int= 2^31 - 1 )
		return "LONGBLOB";
	}

	// the number depth of the approximate log for the triple bloom filters.
	private final int APPROX_LOG_DEPTH = 1;

	// parameter is the page number.
	private static final String CREAT_TABLE_FMT = "CREATE TABLE Page_%s "
			+ "(idx INT AUTO_INCREMENT, hamming INT, log DOUBLE, hash INT,"
			+ "bloom "
			+ getBlobType(TripleBloomFilter.CONFIG.getNumberOfBytes())
			+ ", data BLOB, PRIMARY KEY USING BTREE (idx),"
			+ "INDEX `hashIdx`( hash ),"
			+ "INDEX `hamIdx` USING BTREE (hamming,log) ) " + "ENGINE MyISAM ";

	private static final String CREAT_PAGE_INDEX_TABLE_FMT = "CREATE TABLE PageIndex "
			+ "(idx INT AUTO_INCREMENT, "
			+ "hamming INT, log DOUBLE, "
			+ "bloom "
			+ getBlobType(PageBloomFilter.CONFIG.getNumberOfBytes())
			+ ", PRIMARY KEY USING BTREE (idx ),"
			+ "INDEX `hamIdx` USING BTREE (hamming,log)  ) "
			+ "ENGINE MyISAM "
			+ "PARTITION BY LINEAR KEY( idx ) PARTITIONS 10 ";
	private static final Logger LOG = LoggerFactory
			.getLogger(MySQLCommands.class);

	// call once to create INSERT and once to create UPDATE
	private static final String CREATE_PAGE_INDX_TRIGGER = ""
			+ "CREATE TRIGGER `PageIndex_BEFORE_%1$s`"
			+ "			BEFORE %1$s ON `PageIndex` FOR EACH ROW" + "			BEGIN"
			+ "			     SET NEW.hamming=bloomhamming( new.bloom );"
			+ "			     SET NEW.log=bloomlog(new.bloom);" + "			END";

	// call to create trigger on page creation.
	private static final String CREATE_PAGE_INSERT_TRIGGER = ""
			+ "CREATE TRIGGER `Page_%1$s_AFTER_INSERT`"
			+ "			AFTER INSERT ON `Page_%1$s` FOR EACH ROW"
			+ "			BEGIN"
			+ "				INSERT INTO PageStats SET bytes=length( NEW.`data` ), records=1, deletes=0, idx=%1$s"
			+ "					ON DUPLICATE KEY UPDATE bytes=bytes+VALUES(bytes), records=records+1;"
			+ "			END";

	// call to create stored procedure to add a triple.
	private static final String CREATE_ADD_TRIPLE_PROCEDURE = ""
			+ "CREATE PROCEDURE `add_triple`( IN pageId INTEGER, IN hamming INTEGER , IN log DOUBLE, "
			+ "				IN hash INTEGER, IN tripleFilter BLOB, IN `data` BLOB, IN pageFilter MEDIUMBLOB, "
			+ "				OUT tripleId INTEGER)"
			+ "			BEGIN"
			+ "				SET @s = concat( 'INSERT INTO Page_', pageId,' SET hamming=',hamming,', log=',log,', hash=',hash,', bloom=?, data=?' );"
			+ "			    SET @b = tripleFilter;"
			+ "			    SET @d = `data`;"
			+ "			    PREPARE stmt1 FROM @s;"
			+ "			    EXECUTE stmt1 USING @b, @d;"
			+ "			    SELECT LAST_INSERT_ID() into tripleId;"
			+ "			    DEALLOCATE PREPARE stmt1;"
			+ "			    UPDATE PageIndex SET bloom=bloomupdate( bloom, pageFilter ) where idx=pageid;"
			+ "			END";

	@Override
	public void createSchema(final Connection connection) throws SQLException {
		final DatabaseMetaData metadata = connection.getMetaData();
		final ResultSet rs = null;
		final Statement stmt = null;
		try {
			createPageIndexTable(metadata);
			createPageStatsTable(metadata);
			createProcedures(metadata);

		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	/**
	 * Create any necessary procedures.
	 * 
	 * @param metadata
	 *            the metadata for the database.
	 * @throws SQLException
	 *             on error
	 */
	private void createProcedures(final DatabaseMetaData metadata)
			throws SQLException {
		ResultSet rs = null;
		Statement stmt = null;
		final Connection connection = metadata.getConnection();
		try {
			rs = metadata.getProcedures(connection.getCatalog(),
					connection.getSchema(), "add_triple");
			if (!rs.next()) {
				stmt = connection.createStatement();
				stmt.executeUpdate(CREATE_ADD_TRIPLE_PROCEDURE);
			}
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public String getPageIndexTableName() {
		return "PageIndex";
	}

	/**
	 * Create the page index table.
	 * 
	 * @param metadata
	 *            the metadata for the database.
	 * @throws SQLException
	 */
	private void createPageIndexTable(final DatabaseMetaData metadata)
			throws SQLException {
		ResultSet rs = null;
		Statement stmt = null;
		final Connection connection = metadata.getConnection();
		try {
			rs = metadata.getTables(connection.getCatalog(),
					connection.getSchema(), getPageIndexTableName(),
					new String[] {
						"TABLE"
					});
			if (!rs.next()) {
				// table does not exist
				stmt = connection.createStatement();

				stmt.executeUpdate(CREAT_PAGE_INDEX_TABLE_FMT);
				String stmtStr = String.format(CREATE_PAGE_INDX_TRIGGER,
						"INSERT");
				stmt.executeUpdate(stmtStr);
				stmtStr = String.format(CREATE_PAGE_INDX_TRIGGER, "UPDATE");
				stmt.executeUpdate(stmtStr);
			}
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public int createPage(final Statement stmt) throws SQLException,
			IOException {
		ResultSet rs = null;
		try {
			stmt.executeUpdate("INSERT INTO PageIndex SET bloom=NULL, hamming=0, log=0");
			rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
			if (!rs.next()) {
				throw new IOException("Unable to read created table id");
			}
			final int id = rs.getInt(1);
			String stmtStr = String.format(CREAT_TABLE_FMT, id);
			stmt.execute(stmtStr);
			stmtStr = String.format(CREATE_PAGE_INSERT_TRIGGER, id);
			stmt.execute(stmtStr);
			stmt.executeUpdate(String
					.format("INSERT INTO PageStats SET records=0, deletes=0, bytes=0, idx=%s",
							id));
			return id;
		} finally {
			DbUtils.closeQuietly(rs);
		}
	}

	@Override
	public PreparedStatement pageIndexSearch(final Connection connection,
			final PageBloomFilter filter) throws SQLException {
		PreparedStatement stmt = null;
		try {
			stmt = connection
					.prepareStatement("SELECT bloom, idx FROM PageIndex WHERE hamming>=? AND log>=? AND bloommatch( ?, PageIndex.bloom )");
			stmt.setInt(1, filter.getHammingWeight());
			stmt.setDouble(2, filter.getApproximateLog(0));
			stmt.setBlob(3, DBIO.asInputStream(filter.getByteBuffer()));
			return stmt;
		} catch (final SQLException e) {
			DbUtils.closeQuietly(stmt);
			throw e;
		}
	}

	@Override
	public String pageIndexById() {
		return "SELECT bloom FROM PageIndex WHERE idx=?";
	}

	@Override
	public PreparedStatement tripleSearch(final Connection connection,
			final int pageId, final PageSearchItem candidate)
			throws SQLException, IOException {
		String sql = null;
		PreparedStatement stmt = null;
		try {
			if (candidate.getSerializable().containsWild()) {
				sql = String
						.format("SELECT data, idx FROM Page_%s WHERE hamming>=? AND log>=? AND bloommatch( ?, bloom)",
								pageId);
				stmt = connection.prepareStatement(sql);
				stmt.setInt(1, candidate.getTripleFilter().getHammingWeight());
				stmt.setDouble(2, candidate.getTripleFilter()
						.getApproximateLog(APPROX_LOG_DEPTH));
				stmt.setBlob(3, DBIO.asInputStream(candidate.getTripleFilter()
						.getByteBuffer()));
			}
			else {
				sql = String.format(
						"SELECT data, idx FROM Page_%s WHERE hash=?", pageId);
				stmt = connection.prepareStatement(sql);
				stmt.setInt(1, candidate.getTriple().hashCode());
			}
			LOG.debug("Checking {} with {}", candidate.getTriple(), stmt);
			return stmt;
		} catch (final SQLException e) {
			DbUtils.closeQuietly(stmt);
			throw e;
		} catch (final IOException e) {
			DbUtils.closeQuietly(stmt);
			throw e;
		}
	}

	@Override
	public PreparedStatement tripleScan(final Connection connection,
			final int pageId) throws SQLException {
		final String sql = String.format("SELECT data, idx FROM Page_%s page",
				pageId);
		return connection.prepareStatement(sql);
	}

	@Override
	public int tripleDelete(final Connection connection, final DBPage page,
			final PageSearchItem candidate) throws SQLException, IOException {
		String sql = String.format("DELETE FROM Page_%s WHERE idx=?", page
				.getPageIndex().getId());
		PreparedStatement stmt = null;
		ExtendedIterator<SerializableTriple> iter = null;
		int count = 0;
		try {
			if (candidate.getTriple().equals(Triple.ANY)) {
				sql = String.format("TRUNCATE Page_%s", page.getPageIndex()
						.getId());
				stmt = connection.prepareStatement(sql);
				count = stmt.executeUpdate();
			}
			else {
				stmt = connection.prepareStatement(sql);
				iter = WrappedIterator.create(
						page.new SerializableTripleIterator(candidate))
						.filterKeep(new SerializableTripleFilter(candidate));
				while (iter.hasNext()) {
					stmt.setInt(1, iter.next().getIndex());
					count += stmt.executeUpdate();
				}
			}

			if (count > 0) {
				sql = String.format(
						"UPDATE PageStats SET deletes=deletes+? WHERE idx=%s",
						page.getPageIndex().getId());
				DbUtils.closeQuietly(stmt);
				stmt = connection.prepareStatement(sql);
				stmt.setInt(1, count);
				stmt.executeUpdate();
			}
			return count;
		} finally {
			iter.close();
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public int tripleCount(final Connection connection, final int pageId,
			final TripleBloomFilter filter) throws SQLException, IOException {
		final String sql = String
				.format("SELECT count(*) FROM Page_%%s page WHERE hamming>=? AND log>=? AND bloommatch( ?, page.bloom)",
						pageId);
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.prepareStatement(sql);
			stmt.setInt(1, filter.getHammingWeight());
			stmt.setDouble(2, filter.getApproximateLog(APPROX_LOG_DEPTH));
			stmt.setBlob(3, DBIO.asInputStream(filter.getByteBuffer()));
			rs = stmt.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			}
			else {
				throw new IOException("No result returned from count call");
			}
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public void tripleInsert(final Connection connection, final int pageId,
			final PageSearchItem candidate) throws SQLException, IOException {

		CallableStatement stmt = null;

		try {
			final String simpleProc = "{ call add_triple(?,?,?,?,?,?,?,?) }";
			stmt = connection.prepareCall(simpleProc);
			stmt.setInt(1, pageId);
			stmt.setInt(2, candidate.getTripleFilter().getHammingWeight());
			stmt.setDouble(3, candidate.getTripleFilter().getApproximateLog(3));
			stmt.setInt(4, candidate.getTriple().hashCode());
			stmt.setBlob(5, DBIO.asInputStream(candidate.getTripleFilter()
					.getByteBuffer()));
			stmt.setBlob(6, DBIO.asInputStream(candidate.getSerializable()
					.getByteBuffer()));
			stmt.setBlob(7, DBIO.asInputStream(candidate.getPageFilter()
					.getByteBuffer()));
			stmt.registerOutParameter(8, java.sql.Types.INTEGER);
			stmt.execute();
			candidate.getSerializable().setIndex(stmt.getInt(8));
		} finally {
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public String getPageStatsTableName() {
		return "PageStats";
	}

	/**
	 * Create the page stats table.
	 * 
	 * @param metadata
	 *            the metadata for the database.
	 * @throws SQLException
	 *             on error
	 */
	private void createPageStatsTable(final DatabaseMetaData metadata)
			throws SQLException {
		ResultSet rs = null;
		Statement stmt = null;
		final Connection connection = metadata.getConnection();
		try {
			rs = metadata.getTables(connection.getCatalog(),
					connection.getSchema(), getPageStatsTableName(),
					new String[] {
						"TABLE"
					});
			if (!rs.next()) {
				stmt = connection.createStatement();
				stmt.execute("CREATE TABLE PageStats ( idx INT PRIMARY KEY, records INT, deletes INT, bytes INT ) ENGINE MyISAM");
			}
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
		}
	}

	@Override
	public String getPageCountQuery() {
		return "SELECT COUNT(*) AS size FROM PageStats";
	}

	@Override
	public String statsIncrementRecord(final int tableId) {
		return String.format(
				"UPDATE PageStats SET records=records+1 WHERE idx=%s", tableId);
	}

	@Override
	public String statsIncrementDelete(final int tableId) {
		return String.format(
				"UPDATE PageStats SET deletes=deletes+1 WHERE idx=%s", tableId);
	}

	@Override
	public String statsIncrementBytes(final int tableId, final int bytes) {
		return String.format(
				"UPDATE PageStats SET bytes=bytes+%s WHERE idx=%s", bytes,
				tableId);
	}

	@Override
	public String getStats(final int tableId) {
		return String.format(
				"SELECT records, deletes, bytes FROM PageStats WHERE idx=%s",
				tableId);
	}

	private static final String BEST_PAGE_QUERY = String
			.format("SELECT idx,overs,free FROM "
					+ "(SELECT idx, records-%1$s overs, "
					+ "%1$s-records+deletes free from PageStats order by overs ASC, free DESC) x "
					+ "WHERE free>0 LIMIT 1",
					PageBloomFilter.CONFIG.getNumberOfItems());

	@Override
	public String getBestPageQuery() {
		return BEST_PAGE_QUERY;
	}

	@Override
	public String getRecordCountQuery() {
		return "SUM(records-deletes) AS count FROM PageStats ";
	}
}
