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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.geoname.GeoName;

public abstract class AbstractBigLoadTest {

	Graph graph;

	private static final Logger LOG = LoggerFactory
			.getLogger(AbstractBigLoadTest.class);

	public AbstractBigLoadTest() {
		// TODO Auto-generated constructor stub
	}

	abstract protected Graph getGraph() throws Exception;

	public void setup() throws Exception {
//		LoggingConfig.setConsole(Level.DEBUG);
//		LoggingConfig.setRootLogger(Level.DEBUG);
//		LoggingConfig.setLogger("com.hp.hpl.jena", Level.INFO);
		graph = getGraph();
	}

	/*
	 * geonameid : integer id of record in geonames database name : name of
	 * geographical point (utf8) varchar(200) asciiname : name of geographical
	 * point in plain ascii characters, varchar(200) alternatenames :
	 * alternatenames, comma separated, ascii names automatically
	 * transliterated, convenience attribute from alternatename table,
	 * varchar(10000) latitude : latitude in decimal degrees (wgs84) longitude :
	 * longitude in decimal degrees (wgs84) feature class : see
	 * http://www.geonames.org/export/codes.html, char(1) feature code : see
	 * http://www.geonames.org/export/codes.html, varchar(10) country code :
	 * ISO-3166 2-letter country code, 2 characters cc2 : alternate country
	 * codes, comma separated, ISO-3166 2-letter country code, 60 characters
	 * admin1 code : fipscode (subject to change to iso code), see exceptions
	 * below, see file admin1Codes.txt for display names of this code;
	 * varchar(20) admin2 code : code for the second administrative division, a
	 * county in the US, see file admin2Codes.txt; varchar(80) admin3 code :
	 * code for third level administrative division, varchar(20) admin4 code :
	 * code for fourth level administrative division, varchar(20) population :
	 * bigint (8 byte int) elevation : in meters, integer dem : digital
	 * elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca
	 * 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm
	 * processed by cgiar/ciat. timezone : the timezone id (see file
	 * timeZone.txt) varchar(40) modification date : date of last modification
	 * in yyyy-MM-dd format
	 */

	public void loadData() throws IOException {
		final URL inputFile = AbstractBigLoadTest.class
				.getClassLoader().getResource("allCountries.txt");
		final String uriPattern = "urn:geoname:%s";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(
					inputFile.openStream()));

			for (int i = 0; i < 8000; i++) {

				final GeoName gn = GeoName.parse(br.readLine());
				final Node subject = NodeFactory.createURI(String.format(
						uriPattern, gn.geonameid));

				LOG.info("processing {} {} ", gn.geonameid, i);
				Node predicate = NodeFactory.createURI(String.format(
						uriPattern, "asciiname"));
				Node object = NodeFactory.createLiteral(gn.asciiname);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"latitude"));
				object = NodeFactory.createLiteral(gn.latitude);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"longitude"));
				object = NodeFactory.createLiteral(gn.longitude);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"feature_class"));
				object = NodeFactory.createLiteral(gn.feature_class);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"feature_code"));
				object = NodeFactory.createLiteral(gn.feature_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"country_code"));
				object = NodeFactory.createLiteral(gn.country_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"admin1_code"));
				object = NodeFactory.createLiteral(gn.admin1_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"admin2_code"));
				object = NodeFactory.createLiteral(gn.admin2_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"admin3_code"));
				object = NodeFactory.createLiteral(gn.admin3_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"admin4_code"));
				object = NodeFactory.createLiteral(gn.admin4_code);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"population"));
				object = NodeFactory.createLiteral(gn.population);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"elevation"));
				object = NodeFactory.createLiteral(gn.elevation);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"dem"));
				object = NodeFactory.createLiteral(gn.dem);
				graph.add(new Triple(subject, predicate, object));

				predicate = NodeFactory.createURI(String.format(uriPattern,
						"timezone"));
				object = NodeFactory.createLiteral(gn.timezone);
				graph.add(new Triple(subject, predicate, object));

			}

		} finally {
			IOUtils.closeQuietly(br);
		}

	}
}
