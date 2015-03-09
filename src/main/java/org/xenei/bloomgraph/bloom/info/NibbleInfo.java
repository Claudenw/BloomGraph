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
package org.xenei.bloomgraph.bloom.info;

/**
 * A class that provides bloom filter info about nibbles (1/2 byte).
 *
 */
public class NibbleInfo {

	// the nibble values
	public static NibbleInfo[] NIBBLE_INFO = {
			new NibbleInfo(0, "0000", "0", 0),
			new NibbleInfo(1, "0001", "1", 1),
			new NibbleInfo(2, "0010", "2", 1),
			new NibbleInfo(3, "0011", "3", 2),
			new NibbleInfo(4, "0100", "4", 1),
			new NibbleInfo(5, "0101", "5", 2),
			new NibbleInfo(6, "0110", "6", 2),
			new NibbleInfo(7, "0111", "7", 3),
			new NibbleInfo(8, "1000", "8", 1),
			new NibbleInfo(9, "1001", "9", 2),
			new NibbleInfo(10, "1010", "A", 2),
			new NibbleInfo(11, "1011", "B", 3),
			new NibbleInfo(12, "1100", "C", 2),
			new NibbleInfo(13, "1101", "D", 3),
			new NibbleInfo(14, "1110", "E", 3),
			new NibbleInfo(15, "1111", "F", 4)
	};

	// the value
	private int val;
	// the binary pattern string.
	private String pattern;
	// the hex pattern string.
	private String hexPattern;
	// the hamming weight.
	private int hammingWeight;

	/**
	 * Constructor.
	 * 
	 * @param val
	 *            The value for the nibble.
	 * @param pattern
	 *            The binary pattern for the nibble.
	 * @param hexPattern
	 *            The hex pattern for the nibble.
	 * @param hammingWeight
	 *            The hamming weight for the nibble.
	 */
	private NibbleInfo(int val, String pattern, String hexPattern,
			int hammingWeight) {
		this.val = val;
		this.pattern = pattern;
		this.hexPattern = hexPattern;
		this.hammingWeight = hammingWeight;
	}

	/**
	 * Get the binary pattern.
	 * 
	 * @return The binary pattern
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * Get the hex pattern
	 * 
	 * @return The hex pattern
	 */
	public String getHexPattern() {
		return hexPattern;
	}

	/**
	 * Get the hamming weight.
	 * 
	 * @return the hamming weight.
	 */
	public int getHammingWeight() {
		return hammingWeight;
	}

	/**
	 * Get the value.
	 * 
	 * @return The value.
	 */
	public int getVal() {
		return val;
	}

	@Override
	public int hashCode() {
		return val;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof NibbleInfo) {
			return val == ((NibbleInfo) o).val;
		}
		return false;
	}

	@Override
	public String toString() {
		return hexPattern;
	}
}
