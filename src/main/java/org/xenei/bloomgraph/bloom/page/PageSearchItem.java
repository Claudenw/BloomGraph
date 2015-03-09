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
package org.xenei.bloomgraph.bloom.page;

import java.io.IOException;

import org.xenei.bloomgraph.SerializableTriple;
import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.filters.TripleBloomFilter;

import com.hp.hpl.jena.graph.Triple;

/**
 * The information for a page search. Includes the triple, serializable triple,
 * and the page as well as the triple bloom filter for the triple.
 */
public class PageSearchItem {
	private final Triple triple;
	private SerializableTriple serializable;
	private TripleBloomFilter tripleFilter;
	private PageBloomFilter pageFilter;

	/**
	 * Constructor.
	 * 
	 * @param triple
	 *            the triple we are looking for, may include wild cards.
	 */
	public PageSearchItem(Triple triple) {
		this.triple = triple;
	}

	/**
	 * Get the triple.
	 * 
	 * @return The triple.
	 */
	public Triple getTriple() {
		return triple;
	}

	/**
	 * Get the serializable triple.
	 * 
	 * @return The serializable triple.
	 * @throws IOException
	 *             on error
	 */
	public SerializableTriple getSerializable() throws IOException {
		if (serializable == null) {
			serializable = new SerializableTriple(triple);
		}
		return serializable;
	}

	/**
	 * Get the triple bloom filter.
	 * 
	 * @return the triple bloom filter.
	 * @throws IOException
	 *             on error
	 */
	public TripleBloomFilter getTripleFilter() throws IOException {
		if (tripleFilter == null) {
			tripleFilter = TripleBloomFilter.BUILDER.build(triple);
		}
		return tripleFilter;
	}

	/**
	 * Get the page bloom filter.
	 * 
	 * @return the page bloom filter
	 * @throws IOException
	 *             on error.
	 */
	public PageBloomFilter getPageFilter() throws IOException {
		if (pageFilter == null) {
			pageFilter = PageBloomFilter.BUILDER.build(triple);
		}
		return pageFilter;
	}

}