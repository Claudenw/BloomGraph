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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    List<GeoName> sample;

    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractBigLoadTest.class);

    public AbstractBigLoadTest() {
        // TODO Auto-generated constructor stub
    }

    abstract protected Graph getGraph() throws Exception;

    public void setup() throws Exception {
        graph = getGraph();
    }

    public void loadData(int records) throws IOException {
        final URL inputFile = AbstractBigLoadTest.class
                .getClassLoader().getResource("allCountries.txt");
        BufferedReader br = null;
        sample = new ArrayList<GeoName>();
        try {
            br = new BufferedReader(new InputStreamReader(
                    inputFile.openStream()));

            for (int i = 0; i < records; i++) {

                final GeoName gn = GeoName.parse(br.readLine());
                if ( i % 100 == 0)
                {
                    sample.add( gn );
                }

                Map<String,Triple> data = parse( gn );
                data.values().stream().forEach( graph::add );
            }

        } finally {
            IOUtils.closeQuietly(br);
        }

    }

    public static Map<String,Triple> parse(GeoName gn)
    {
        final String uriPattern = "urn:geoname:%s";
        final Node subject = NodeFactory.createURI(String.format(
                uriPattern, gn.geonameid));
        Map<String,Triple> data = new HashMap<String,Triple>();

        LOG.info("processing {} ", gn.geonameid);
        Node predicate = NodeFactory.createURI(String.format(
                uriPattern, "asciiname"));
        Node object = NodeFactory.createLiteral(gn.asciiname);
        data.put( "asciiname", new Triple(subject, predicate, object));


        predicate = NodeFactory.createURI(String.format(uriPattern,
                "latitude"));
        object = NodeFactory.createLiteral(gn.latitude);
        data.put( "latitude", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "longitude"));
        object = NodeFactory.createLiteral(gn.longitude);
        data.put( "longitude", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "feature_class"));
        object = NodeFactory.createLiteral(gn.feature_class);
        data.put( "feature_class", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "feature_code"));
        object = NodeFactory.createLiteral(gn.feature_code);
        data.put( "feature_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "country_code"));
        object = NodeFactory.createLiteral(gn.country_code);
        data.put( "country_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "admin1_code"));
        object = NodeFactory.createLiteral(gn.admin1_code);
        data.put( "admin1_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "admin2_code"));
        object = NodeFactory.createLiteral(gn.admin2_code);
        data.put( "admin2_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "admin3_code"));
        object = NodeFactory.createLiteral(gn.admin3_code);
        data.put( "admin3_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "admin4_code"));
        object = NodeFactory.createLiteral(gn.admin4_code);
        data.put( "admin4_code", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "population"));
        object = NodeFactory.createLiteral(gn.population);
        data.put( "population", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "elevation"));
        object = NodeFactory.createLiteral(gn.elevation);
        data.put( "elevation", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "dem"));
        object = NodeFactory.createLiteral(gn.dem);
        data.put( "dem", new Triple(subject, predicate, object));

        predicate = NodeFactory.createURI(String.format(uriPattern,
                "timezone"));
        object = NodeFactory.createLiteral(gn.timezone);
        data.put( "timezone", new Triple(subject, predicate, object));

        return data;
    }
}
