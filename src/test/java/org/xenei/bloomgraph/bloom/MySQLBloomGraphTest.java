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

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.xenei.bloomgraph.bloom.sql.DBIO;
import org.xenei.bloomgraph.bloom.sql.MySQLCommands;
import org.xenei.bloomgraph.bloom.sql.SQLCommands;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MySQLBloomGraphTest extends AbstractBloomGraphTest {

	public static final String URL = "jdbc:mysql://127.0.0.1:3306/bloomTest";
	public static final String USR = "root";
	public static final String PWD = "foobar";

	public MySQLBloomGraphTest() {
		// TODO Auto-generated constructor stub
	}

	@Override
	protected BloomIO getBloomIO() throws SQLException {
		final DataSource ds = getMySQLDataSource(URL, USR, PWD);
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

	public static DataSource getMySQLDataSource(final String url,
			final String user, final String pwd) {
		final Properties props = new Properties();
		final FileInputStream fis = null;
		MysqlDataSource mysqlDS = null;
		// try {
		// fis = new FileInputStream("db.properties");
		// props.load(fis);

		mysqlDS = new MysqlDataSource();
		// mysqlDS.setURL(props.getProperty("MYSQL_DB_URL"));
		// mysqlDS.setUser(props.getProperty("MYSQL_DB_USERNAME"));
		// mysqlDS.setPassword(props.getProperty("MYSQL_DB_PASSWORD"));
		mysqlDS.setURL(url);
		mysqlDS.setUser(user);
		mysqlDS.setPassword(pwd);
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		return mysqlDS;
	}

}
