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
package org.xenei.bloomgraph.bloom.mem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.filters.TripleBloomFilter;
import org.xenei.bloomgraph.bloom.index.AbstractIndex;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;
import org.xenei.bloomgraph.bloom.page.UpdatablePageStatistics;

import com.hp.hpl.jena.util.iterator.ClosableIterator;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * A triple index list.
 *
 */
public class TripleIndexList {

	public static final Logger LOG = LoggerFactory
			.getLogger(TripleIndexList.class);
	/**
	 * The buffer for all the index records
	 */
	private final List<TripleIndex> indexBuffer;

	// the updatable page statistics.
	private final UpdatablePageStatistics statistics;

	/**
	 * Constructor.
	 * 
	 * @param statistics
	 *            The updatable statistics object.
	 */
	public TripleIndexList(final UpdatablePageStatistics statistics) {
		this.indexBuffer = new ArrayList<TripleIndex>(
				PageBloomFilter.CONFIG.getNumberOfItems());
		this.statistics = statistics;
	}

	/**
	 * Add an index to the page.
	 * 
	 * @param bloomFilter
	 *            the bloom filter to add
	 * @param idx
	 *            The id to associate the filter with.
	 * @param offset
	 *            The offset into the buffer of the filter..
	 */
	public void addIndex(final TripleBloomFilter bloomFilter, final int idx,
			final int offset) {
		final TripleIndex pageIndex = new TripleIndex(bloomFilter, idx, offset);
		while (indexBuffer.size() < pageIndex.getId()) {
			indexBuffer.add(null);
		}
		if (indexBuffer.size() == pageIndex.getId()) {
			indexBuffer.add(pageIndex);
		}
		else {
			indexBuffer.set(pageIndex.getId(), pageIndex);
		}
	}

	/**
	 * Te the triple index at the id.
	 * 
	 * @param idx
	 *            the id to get
	 * @return The triple index.
	 */
	public TripleIndex getIndex(final int idx) {
		return indexBuffer.get(idx);
	}

	/**
	 * Get an iterator on the triple indexes.
	 * 
	 * @return the iterator.
	 * @throws IOException
	 *             on error
	 */
	public ClosableIterator<TripleIndex> iterator() throws IOException {
		return new TripleIndexIterator();
	}

	/**
	 * An iterator on the TripleIndex that is filtered by the target. only
	 * indexes that match the target will be returned.
	 * 
	 * @param target
	 *            the target to match.
	 * @return the iterator.
	 * @throws IOException
	 */
	public ExtendedIterator<TripleIndex> iterator(final PageSearchItem target)
			throws IOException {
		return WrappedIterator.create(new TripleIndexIterator()).filterKeep(
				new TripleIndexFilter(target.getTripleFilter()));
	}

	/**
	 * A triple index.
	 *
	 */
	public class TripleIndex extends AbstractIndex<TripleBloomFilter> {
		// the offset into the buffer.
		private final int offset;

		/**
		 * Constructor
		 * 
		 * @param bloomFilter
		 *            The bloom filter.
		 * @param id
		 *            the id of the filter
		 * @param offset
		 *            the offset of the filter.
		 */
		private TripleIndex(final TripleBloomFilter bloomFilter, final int id,
				final int offset) {
			super(bloomFilter, id);
			this.offset = offset;
		}

		@Override
		protected TripleBloomFilter createFilter(final ByteBuffer buff) {
			return new TripleBloomFilter(buff);
		}

		@Override
		public void delete() {
			LOG.debug("Deleting index record {} for offset {}", getId(), offset);
			doDelete();
			statistics.incrementDeleteCount();
		}

		/**
		 * Get the offset of the filter.
		 * 
		 * @return the offset into the buffer for the filter
		 */
		public int getOffset() {
			return offset;
		}
	}

	/**
	 * A triple index iterator
	 *
	 */
	private class TripleIndexIterator extends
			AbstractIndexIterator<TripleIndex> {
		private int pos;

		private TripleIndexIterator() throws IOException {
			pos = 0;
		}

		@Override
		protected TripleIndex findNext() {
			TripleIndex retval = null;
			while (pos < statistics.getRecordCount()) {
				retval = getIndex(pos++);
				if (retval.getFilter().getHammingWeight() != 0) {
					return retval;
				}
			}
			return null;
		}
	}

	/**
	 * The triple index filter. Filters TripleIndexes by the bloom filter.
	 *
	 */
	private class TripleIndexFilter extends Filter<TripleIndex> {
		// the bloom filter to match
		private final TripleBloomFilter target;

		/**
		 * Constructor
		 * 
		 * @param target
		 *            The bloom filter to match.
		 */
		public TripleIndexFilter(final TripleBloomFilter target) {
			this.target = target;
		}

		@Override
		public boolean accept(final TripleIndex o) {
			return target.match(o.getFilter());
		}
	}

}
