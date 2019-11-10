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

import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.apache.jena.graph.Graph;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloomgraph.BloomTriple;

public class BloomBigLoadTest extends AbstractBigLoadTest {

	public BloomBigLoadTest() {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
	}

	@Override
	protected final Graph getGraph() throws Exception {
		Shape shape = new Shape( Murmur128.NAME, 3, 1.0/30000000 );
		Storage<BloomTriple> storage = new InMemory<BloomTriple>();
		Index index = new FlatBloofi( shape );
		Container<BloomTriple> container = new ContainerImpl<BloomTriple>( shape, storage, index );
		return new BloomGraph( container );
	}

	public static void main(final String[] args) throws Exception {
		final BloomBigLoadTest test = new BloomBigLoadTest();
		test.setup();
		test.loadData();
	}
}
