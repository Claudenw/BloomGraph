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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A class that provides bloom filter info about bytes.
 *
 */
public class ByteInfo {
	/**
	 * the value
	 */
	private byte val;
	/**
	 * The hamming weight
	 */
	private int hammingWeight;

	/**
	 * The list of ByteInfo objects for each byte.
	 */
	public static ByteInfo[] BYTE_INFO = new ByteInfo[256];

	// populate the byte array.
	static {
		for (int i = 0; i < 256; i++) {
			BYTE_INFO[i] = new ByteInfo((byte) i);
		}
	}

	/**
	 * Get the hex string for the byte buffer.
	 * 
	 * @param bb
	 *            the byte buffer to create the string for.
	 * @return The string of hex values.
	 */
	public static String toHexString(ByteBuffer bb) {
		StringBuilder sb = new StringBuilder(bb.limit());
		if (bb.order() == ByteOrder.BIG_ENDIAN) {
			for (int i = 0; i < bb.limit(); i++) {
				sb.append(BYTE_INFO[bb.get(i) & 0xFF].getHexPattern());
			}
		}
		else {
			for (int i = bb.limit() - 1; i >= 0; i--) {
				sb.append(BYTE_INFO[bb.get(i) & 0xFF].getHexPattern());
			}
		}
		return sb.toString();
	}

	/**
	 * Get the binary representation of the byte buffer.
	 * 
	 * @param bb
	 *            The byte buffer to create the string for.
	 * @return the string of binary representations.
	 */
	public static String toBinaryString(ByteBuffer bb) {
		StringBuilder sb = new StringBuilder(bb.limit() * 8);
		if (bb.order() == ByteOrder.BIG_ENDIAN) {
			for (int i = 0; i < bb.limit(); i++) {
				sb.append(BYTE_INFO[bb.get(i) & 0xFF].getPattern());
			}
		}
		else {
			for (int i = bb.limit() - 1; i >= 0; i--) {
				sb.append(BYTE_INFO[bb.get(i) & 0xFF].getPattern());
			}
		}
		return sb.toString();
	}

	/**
	 * Create the byte info for the byte.
	 * 
	 * @param val
	 *            The byte.
	 */
	private ByteInfo(byte val) {
		this.val = val;
		int x = 0x0F & (val >> 4);
		hammingWeight = NibbleInfo.NIBBLE_INFO[x].getHammingWeight();
		x = 0x0F & val;
		hammingWeight += NibbleInfo.NIBBLE_INFO[x].getHammingWeight();
	}

	/**
	 * Get the binary pattern for the byte.
	 * 
	 * @return the binary pattern
	 */
	public String getPattern() {
		int x = 0x0F & (val >> 4);
		StringBuilder retval = new StringBuilder(
				NibbleInfo.NIBBLE_INFO[x].getPattern());
		x = 0x0F & val;
		retval.append(NibbleInfo.NIBBLE_INFO[x].getPattern());
		return retval.toString();
	}

	/**
	 * Get the nibbles for the bye
	 * 
	 * @return the two nibles that comprise the byte.
	 */
	public NibbleInfo[] getNibbles() {
		int x = 0x0F & (val >> 4);
		NibbleInfo ni1 = NibbleInfo.NIBBLE_INFO[x];
		x = 0x0F & val;
		NibbleInfo ni2 = NibbleInfo.NIBBLE_INFO[x];
		return new NibbleInfo[] {
				ni1, ni2
		};
	}

	/**
	 * Get the hext pattern for the byte
	 * 
	 * @return The hex pattern as a string.
	 */
	public String getHexPattern() {
		int x = 0x0F & (val >> 4);
		StringBuilder retval = new StringBuilder(
				NibbleInfo.NIBBLE_INFO[x].getHexPattern());
		x = 0x0F & val;
		retval.append(NibbleInfo.NIBBLE_INFO[x].getHexPattern());
		return retval.toString();
	}

	/**
	 * get the hamming weight for the byte.
	 * 
	 * @return The hamming weight for the byte.
	 */
	public int getHammingWeight() {
		return hammingWeight;
	}

	/**
	 * Get the byte as an unsigned value.
	 * 
	 * @return the unsigned byte value.
	 */
	public int getVal() {
		return 0xFF & val;
	}

	@Override
	public int hashCode() {
		return val;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ByteInfo) {
			return val == ((ByteInfo) o).val;
		}
		return false;
	}

	@Override
	public String toString() {
		return getHexPattern();
	}

}
