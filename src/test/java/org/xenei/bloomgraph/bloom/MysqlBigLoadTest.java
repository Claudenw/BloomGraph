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
package org.xenei.bloomgraph.bloom;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Level;
import org.xenei.bloomgraph.bloom.sql.DBIO;
import org.xenei.bloomgraph.bloom.sql.MySQLCommands;
import org.xenei.bloomgraph.bloom.sql.SQLCommands;

public class MysqlBigLoadTest extends BloomBigLoadTest {

	public MysqlBigLoadTest() {
		// TODO Auto-generated constructor stub
	}

	public static final String URL = "jdbc:mysql://127.0.0.1:3306/newBloom";
	public static final String USR = "root";
	public static final String PWD = "foobar";

	@Override
	protected BloomIO getBloomIO() throws SQLException {
		LoggingConfig.setLogger("org.xenei.bloomgraph.bloom", Level.INFO);

		final DataSource ds = MySQLBloomGraphTest.getMySQLDataSource(URL, USR,
				PWD);
		final Connection c = ds.getConnection();
		final Statement stmt = c.createStatement();
		ResultSet rs = null;
		try {
			final SQLCommands sqlCmd = new MySQLCommands();
			final DatabaseMetaData metaData = c.getMetaData();
			rs = metaData.getTables(c.getCatalog(), c.getSchema(),
					sqlCmd.getPageIndexTableName(), new String[] {
				"TABLE"
			});
			while (rs.next()) {
				stmt.execute("TRUNCATE " + rs.getString(3));
			}
			DbUtils.closeQuietly(rs);
			rs = metaData.getTables(c.getCatalog(), c.getSchema(),
					sqlCmd.getPageStatsTableName(), new String[] {
				"TABLE"
			});
			while (rs.next()) {
				stmt.execute("TRUNCATE " + rs.getString(3));
			}
			DbUtils.closeQuietly(rs);
			rs = metaData.getTables(c.getCatalog(), c.getSchema(), "Page\\_%",
					new String[] {
				"TABLE"
			});
			while (rs.next()) {
				stmt.execute("DROP TABLE " + rs.getString(3));
			}
			return new DBIO(ds, sqlCmd);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(c);
		}
	}

	public static void main(final String[] args) throws Exception {
		final MysqlBigLoadTest test = new MysqlBigLoadTest();
		test.setup();
		test.loadData();
	}
}
