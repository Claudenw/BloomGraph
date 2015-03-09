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

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.TDBFactory;

public class TDBBigLoadTest extends AbstractBigLoadTest {

	public TDBBigLoadTest() {
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Graph getGraph() throws Exception {
		final Dataset ds = TDBFactory.createDataset("/tmp/tdb");
		return ds.getDefaultModel().getGraph();
	}

	public static void main(final String[] args) throws Exception {
		final TDBBigLoadTest test = new TDBBigLoadTest();
		test.setup();
		test.loadData();
	}
}
