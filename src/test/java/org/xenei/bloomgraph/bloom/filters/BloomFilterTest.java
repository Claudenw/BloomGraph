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

import java.io.IOException;
import java.util.BitSet;

import org.junit.Test;
import org.xenei.bloomgraph.bloom.info.ByteInfo;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

public class BloomFilterTest {
	TripleBloomFilter bloomFilter;

	@Test
	public void testApproximateLog() throws IOException {
		// this test is sensitive to change in bloom filter calculations
		final Node n = NodeFactory.createURI("urn:test");
		final TripleBloomFilter bloomFilter = TripleBloomFilter.BUILDER.build(
				n, n, n);
		assertEquals(71, bloomFilter.getApproximateLog(0), 0.000001);
		assertEquals(71.00390625, bloomFilter.getApproximateLog(1), 0.000001);
		assertEquals(71.0048828125, bloomFilter.getApproximateLog(2), 0.000001);
		assertEquals(71.00494384765625, bloomFilter.getApproximateLog(3),
				0.000001);
		assertEquals(71.00494480133057, bloomFilter.getApproximateLog(4),
				0.000001);
		assertEquals(71.00494503974915, bloomFilter.getApproximateLog(5),
				0.000001);
		assertEquals(71.00494503974915, bloomFilter.getApproximateLog(6),
				0.000001); // at this point there is no change.
	}

	@Test
	public void testApproximateLogOnByteInfo() throws IOException {
		final double[][] data = {
				{
					0.0, 0.0, 0.0, 0.0, 0.0
				}, {
					0.0, 0.0, 0.0, 0.0, 0.0
				}, {
					1.0, 1.0, 1.0, 1.0, 1.0
				}, {
					1.0, 1.5, 1.5, 1.5, 1.5
				}, {
					2.0, 2.0, 2.0, 2.0, 2.0
				}, {
					2.0, 2.25, 2.25, 2.25, 2.25
				}, {
					2.0, 2.5, 2.5, 2.5, 2.5
				}, {
					2.0, 2.5, 2.75, 2.75, 2.75
				}, {
					3.0, 3.0, 3.0, 3.0, 3.0
				}, {
					3.0, 3.125, 3.125, 3.125, 3.125
				}, {
					3.0, 3.25, 3.25, 3.25, 3.25
				}, {
					3.0, 3.25, 3.375, 3.375, 3.375
				}, {
					3.0, 3.5, 3.5, 3.5, 3.5
				}, {
					3.0, 3.5, 3.625, 3.625, 3.625
				}, {
					3.0, 3.5, 3.75, 3.75, 3.75
				}, {
					3.0, 3.5, 3.75, 3.875, 3.875
				}, {
					4.0, 4.0, 4.0, 4.0, 4.0
				}, {
					4.0, 4.0625, 4.0625, 4.0625, 4.0625
				}, {
					4.0, 4.125, 4.125, 4.125, 4.125
				}, {
					4.0, 4.125, 4.1875, 4.1875, 4.1875
				}, {
					4.0, 4.25, 4.25, 4.25, 4.25
				}, {
					4.0, 4.25, 4.3125, 4.3125, 4.3125
				}, {
					4.0, 4.25, 4.375, 4.375, 4.375
				}, {
					4.0, 4.25, 4.375, 4.4375, 4.4375
				}, {
					4.0, 4.5, 4.5, 4.5, 4.5
				}, {
					4.0, 4.5, 4.5625, 4.5625, 4.5625
				}, {
					4.0, 4.5, 4.625, 4.625, 4.625
				}, {
					4.0, 4.5, 4.625, 4.6875, 4.6875
				}, {
					4.0, 4.5, 4.75, 4.75, 4.75
				}, {
					4.0, 4.5, 4.75, 4.8125, 4.8125
				}, {
					4.0, 4.5, 4.75, 4.875, 4.875
				}, {
					4.0, 4.5, 4.75, 4.875, 4.9375
				}, {
					5.0, 5.0, 5.0, 5.0, 5.0
				}, {
					5.0, 5.03125, 5.03125, 5.03125, 5.03125
				}, {
					5.0, 5.0625, 5.0625, 5.0625, 5.0625
				}, {
					5.0, 5.0625, 5.09375, 5.09375, 5.09375
				}, {
					5.0, 5.125, 5.125, 5.125, 5.125
				}, {
					5.0, 5.125, 5.15625, 5.15625, 5.15625
				}, {
					5.0, 5.125, 5.1875, 5.1875, 5.1875
				}, {
					5.0, 5.125, 5.1875, 5.21875, 5.21875
				}, {
					5.0, 5.25, 5.25, 5.25, 5.25
				}, {
					5.0, 5.25, 5.28125, 5.28125, 5.28125
				}, {
					5.0, 5.25, 5.3125, 5.3125, 5.3125
				}, {
					5.0, 5.25, 5.3125, 5.34375, 5.34375
				}, {
					5.0, 5.25, 5.375, 5.375, 5.375
				}, {
					5.0, 5.25, 5.375, 5.40625, 5.40625
				}, {
					5.0, 5.25, 5.375, 5.4375, 5.4375
				}, {
					5.0, 5.25, 5.375, 5.4375, 5.46875
				}, {
					5.0, 5.5, 5.5, 5.5, 5.5
				}, {
					5.0, 5.5, 5.53125, 5.53125, 5.53125
				}, {
					5.0, 5.5, 5.5625, 5.5625, 5.5625
				}, {
					5.0, 5.5, 5.5625, 5.59375, 5.59375
				}, {
					5.0, 5.5, 5.625, 5.625, 5.625
				}, {
					5.0, 5.5, 5.625, 5.65625, 5.65625
				}, {
					5.0, 5.5, 5.625, 5.6875, 5.6875
				}, {
					5.0, 5.5, 5.625, 5.6875, 5.71875
				}, {
					5.0, 5.5, 5.75, 5.75, 5.75
				}, {
					5.0, 5.5, 5.75, 5.78125, 5.78125
				}, {
					5.0, 5.5, 5.75, 5.8125, 5.8125
				}, {
					5.0, 5.5, 5.75, 5.8125, 5.84375
				}, {
					5.0, 5.5, 5.75, 5.875, 5.875
				}, {
					5.0, 5.5, 5.75, 5.875, 5.90625
				}, {
					5.0, 5.5, 5.75, 5.875, 5.9375
				}, {
					5.0, 5.5, 5.75, 5.875, 5.9375
				}, {
					6.0, 6.0, 6.0, 6.0, 6.0
				}, {
					6.0, 6.015625, 6.015625, 6.015625, 6.015625
				}, {
					6.0, 6.03125, 6.03125, 6.03125, 6.03125
				}, {
					6.0, 6.03125, 6.046875, 6.046875, 6.046875
				}, {
					6.0, 6.0625, 6.0625, 6.0625, 6.0625
				}, {
					6.0, 6.0625, 6.078125, 6.078125, 6.078125
				}, {
					6.0, 6.0625, 6.09375, 6.09375, 6.09375
				}, {
					6.0, 6.0625, 6.09375, 6.109375, 6.109375
				}, {
					6.0, 6.125, 6.125, 6.125, 6.125
				}, {
					6.0, 6.125, 6.140625, 6.140625, 6.140625
				}, {
					6.0, 6.125, 6.15625, 6.15625, 6.15625
				}, {
					6.0, 6.125, 6.15625, 6.171875, 6.171875
				}, {
					6.0, 6.125, 6.1875, 6.1875, 6.1875
				}, {
					6.0, 6.125, 6.1875, 6.203125, 6.203125
				}, {
					6.0, 6.125, 6.1875, 6.21875, 6.21875
				}, {
					6.0, 6.125, 6.1875, 6.21875, 6.234375
				}, {
					6.0, 6.25, 6.25, 6.25, 6.25
				}, {
					6.0, 6.25, 6.265625, 6.265625, 6.265625
				}, {
					6.0, 6.25, 6.28125, 6.28125, 6.28125
				}, {
					6.0, 6.25, 6.28125, 6.296875, 6.296875
				}, {
					6.0, 6.25, 6.3125, 6.3125, 6.3125
				}, {
					6.0, 6.25, 6.3125, 6.328125, 6.328125
				}, {
					6.0, 6.25, 6.3125, 6.34375, 6.34375
				}, {
					6.0, 6.25, 6.3125, 6.34375, 6.359375
				}, {
					6.0, 6.25, 6.375, 6.375, 6.375
				}, {
					6.0, 6.25, 6.375, 6.390625, 6.390625
				}, {
					6.0, 6.25, 6.375, 6.40625, 6.40625
				}, {
					6.0, 6.25, 6.375, 6.40625, 6.421875
				}, {
					6.0, 6.25, 6.375, 6.4375, 6.4375
				}, {
					6.0, 6.25, 6.375, 6.4375, 6.453125
				}, {
					6.0, 6.25, 6.375, 6.4375, 6.46875
				}, {
					6.0, 6.25, 6.375, 6.4375, 6.46875
				}, {
					6.0, 6.5, 6.5, 6.5, 6.5
				}, {
					6.0, 6.5, 6.515625, 6.515625, 6.515625
				}, {
					6.0, 6.5, 6.53125, 6.53125, 6.53125
				}, {
					6.0, 6.5, 6.53125, 6.546875, 6.546875
				}, {
					6.0, 6.5, 6.5625, 6.5625, 6.5625
				}, {
					6.0, 6.5, 6.5625, 6.578125, 6.578125
				}, {
					6.0, 6.5, 6.5625, 6.59375, 6.59375
				}, {
					6.0, 6.5, 6.5625, 6.59375, 6.609375
				}, {
					6.0, 6.5, 6.625, 6.625, 6.625
				}, {
					6.0, 6.5, 6.625, 6.640625, 6.640625
				}, {
					6.0, 6.5, 6.625, 6.65625, 6.65625
				}, {
					6.0, 6.5, 6.625, 6.65625, 6.671875
				}, {
					6.0, 6.5, 6.625, 6.6875, 6.6875
				}, {
					6.0, 6.5, 6.625, 6.6875, 6.703125
				}, {
					6.0, 6.5, 6.625, 6.6875, 6.71875
				}, {
					6.0, 6.5, 6.625, 6.6875, 6.71875
				}, {
					6.0, 6.5, 6.75, 6.75, 6.75
				}, {
					6.0, 6.5, 6.75, 6.765625, 6.765625
				}, {
					6.0, 6.5, 6.75, 6.78125, 6.78125
				}, {
					6.0, 6.5, 6.75, 6.78125, 6.796875
				}, {
					6.0, 6.5, 6.75, 6.8125, 6.8125
				}, {
					6.0, 6.5, 6.75, 6.8125, 6.828125
				}, {
					6.0, 6.5, 6.75, 6.8125, 6.84375
				}, {
					6.0, 6.5, 6.75, 6.8125, 6.84375
				}, {
					6.0, 6.5, 6.75, 6.875, 6.875
				}, {
					6.0, 6.5, 6.75, 6.875, 6.890625
				}, {
					6.0, 6.5, 6.75, 6.875, 6.90625
				}, {
					6.0, 6.5, 6.75, 6.875, 6.90625
				}, {
					6.0, 6.5, 6.75, 6.875, 6.9375
				}, {
					6.0, 6.5, 6.75, 6.875, 6.9375
				}, {
					6.0, 6.5, 6.75, 6.875, 6.9375
				}, {
					6.0, 6.5, 6.75, 6.875, 6.9375
				}
		};
		for (int i = 0; i < 128; i++) {
			final double[] values = data[i];
			final BF bf = new BF(ByteInfo.BYTE_INFO[i]);
			for (int j = 0; j < 5; j++) {
				assertEquals(values[j], bf.getApproximateLog(j), 0.000001);
			}
		}
	}

	private class BF extends AbstractBloomFilter {
		public BF(final ByteInfo bi) {
			super(BitSet.valueOf(new byte[] {
					(byte) bi.getVal()
			}));
		}

		@Override
		public int getSize() {
			return 1;
		}
	}

}
