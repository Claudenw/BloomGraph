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
package org.xenei.bloomgraph.bloom.filters;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BloomFilterConfigTest {

	AbstractBloomFilter.FilterConfig cfg;

	@Test
	public void testStandardTriple() {
		cfg = new AbstractBloomFilter.FilterConfig(3, 100000);
		assertEquals(3, cfg.getNumberOfItems());
		assertEquals(100000, cfg.getProbability());
		assertEquals(72, cfg.getNumberOfBits());
		assertEquals(9, cfg.getNumberOfBytes());
		assertEquals(17, cfg.getNumberOfHashFunctions());
	}

	@Test
	public void testStandardPage() {
		cfg = new AbstractBloomFilter.FilterConfig(100000, 10000);
		assertEquals(100000, cfg.getNumberOfItems());
		assertEquals(10000, cfg.getProbability());
		assertEquals(1917012, cfg.getNumberOfBits());
		assertEquals(239627, cfg.getNumberOfBytes());
		assertEquals(13, cfg.getNumberOfHashFunctions());
	}

}
