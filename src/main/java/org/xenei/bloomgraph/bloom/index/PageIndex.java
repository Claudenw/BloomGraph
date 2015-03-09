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
package org.xenei.bloomgraph.bloom.index;

import java.nio.ByteBuffer;

import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;

/**
 * A page index associates an id with a page bloom filter.
 *
 */
public class PageIndex extends AbstractIndex<PageBloomFilter> {
	/**
	 * Constructor.
	 * 
	 * @param idx
	 *            The id to associate with page bloom filter.
	 */
	public PageIndex(int idx) {
		super(idx);
	}

	/**
	 * Constructor with buffer and id.
	 * 
	 * @param buff
	 *            the buffer to create the PageBloomFilter from.
	 * @param idx
	 *            the id to associate the filter with.
	 */
	public PageIndex(ByteBuffer buff, int idx) {
		super(buff, idx);
	}

	@Override
	protected PageBloomFilter createFilter(ByteBuffer buff) {
		if (buff == null) {
			return new PageBloomFilter();
		}
		return new PageBloomFilter(buff);
	}

	/**
	 * Delete the values from the filter.
	 */
	public void delete() {
		doDelete();
	}

}
