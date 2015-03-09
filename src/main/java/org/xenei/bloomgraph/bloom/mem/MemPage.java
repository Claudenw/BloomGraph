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
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.SerializableTriple;
import org.xenei.bloomgraph.bloom.filters.PageBloomFilter;
import org.xenei.bloomgraph.bloom.filters.TripleBloomFilter;
import org.xenei.bloomgraph.bloom.index.PageIndex;
import org.xenei.bloomgraph.bloom.mem.TripleIndexList.TripleIndex;
import org.xenei.bloomgraph.bloom.page.AbstractPage;
import org.xenei.bloomgraph.bloom.page.PageSearchItem;
import org.xenei.bloomgraph.bloom.page.SerializableTripleFilter;
import org.xenei.bloomgraph.bloom.page.UpdatablePageStatistics;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

/**
 * A memory based page implementation.
 *
 */
public class MemPage extends AbstractPage {
	private final Logger log;

	/**
	 * The complete buffer
	 */
	private ByteBuffer dataBuffer;
	/**
	 * The index for Triples
	 */
	private final TripleIndexList tripleIndex;

	// an updatable version of the statistics
	private final UpdatablePageStatistics statistics;

	/**
	 * Create a memory page that is associated with the page index.
	 * 
	 * @param pageIndex
	 *            the index this page is associated with.
	 */
	public MemPage(final PageIndex pageIndex) {
		super(pageIndex);
		this.statistics = new Statistics();
		this.tripleIndex = new TripleIndexList(statistics);
		this.dataBuffer = ByteBuffer.allocate(PageBloomFilter.CONFIG
				.getNumberOfItems());
		this.log = LoggerFactory.getLogger(String.format("%s.%s",
				MemPage.class.getName(), pageIndex.getId()));
	}

	@Override
	protected void lock() {
		debug("Lock");
	};

	@Override
	protected void unlock() {
		debug("Unlock");
	};

	@Override
	protected void flush() {
		debug("Flush");
	};

	@Override
	public UpdatablePageStatistics getUpdatableStatistics() {
		return statistics;
	}

	@Override
	public ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException {

		ExtendedIterator<SerializableTriple> inner = WrappedIterator.create(
				tripleIndex.iterator(candidate)).mapWith(
				new SerializableTripleMap());

		if (!candidate.getTriple().equals(Triple.ANY)) {
			inner = inner.filterKeep(new SerializableTripleFilter(candidate));
		}
		return inner.mapWith(new Map1<SerializableTriple, Triple>() {

			@Override
			public Triple map1(final SerializableTriple o) {
				try {
					return o.getTriple();
				} catch (final IOException e) {
					throw new IllegalArgumentException(e.getMessage(), e);
				}
			}
		});
	}

	/**
	 * Return an approximate count of the number of records that will match the
	 * candidate
	 *
	 * @param candidate
	 * @return
	 * @throws IOException
	 */
	@Override
	public int doCount(final PageSearchItem candidate) throws IOException {

		final Iterator<TripleIndex> iter = tripleIndex.iterator(candidate);
		int result = 0;
		while (iter.hasNext()) {
			result++;
			iter.next();
		}
		return result;
	}

	/**
	 * returns false if the page is full
	 *
	 * @param candidate
	 *            the PageSearchItem to write
	 * @return true if the triple is on the page, false is there is no space.
	 * @throws IOException
	 */
	@Override
	public boolean doWrite(final PageSearchItem candidate) throws IOException {

		final SerializableTriple st = candidate.getSerializable();
		final TripleBloomFilter tbf = candidate.getTripleFilter();
		lock();
		try {
			final int nextId = statistics.getRecordCount();
			if (nextId == PageBloomFilter.CONFIG.getNumberOfItems()) {
				log.warn("Page full");
				return false;
			}
			st.setIndex(statistics.getRecordCount());
			final int offset = statistics.getDataSize();
			final int dataSize = st.getSize() + Integer.BYTES;
			ensureDatabufferSpace(offset + dataSize);
			log.debug("Writing {} data bytes at offset {} ", dataSize, offset);
			dataBuffer.position(offset);
			dataBuffer.putInt(st.getSize());
			dataBuffer.put((ByteBuffer) st.getByteBuffer().position(0));

			tripleIndex.addIndex(tbf, nextId, offset);

			statistics.incrementRecordCount();
			statistics.incrementDataSize(dataSize);
			log.debug("Updating page filter");
			pageIndex.getFilter().add(candidate.getPageFilter());
			flush();
			return true;
		} finally {
			unlock();
		}
	}

	/**
	 * Ensure that we have enough space in the buffer.
	 * 
	 * @param minSize
	 *            The minimum size for the buffer.
	 */
	private void ensureDatabufferSpace(final int minSize) {
		log.debug("Ensuring dataspace: {}", minSize);
		if (dataBuffer.limit() >= minSize) {
			return;
		}
		// resize the buffer
		log.info("Resizing buffer");
		final int recordCount = statistics.getRecordCount();
		// estimate new size
		final int avgSize = dataBuffer.limit() / recordCount;

		int newSize = avgSize * PageBloomFilter.CONFIG.getNumberOfItems();

		// check the newsize is big enough.
		if (newSize < minSize) {
			// // not big enough -- size greater than minSize
			// // calc size of other entries and add min size
			final int diff = minSize - statistics.getDataSize();
			newSize = dataBuffer.limit()
					+ (diff * (PageBloomFilter.CONFIG.getNumberOfItems() - recordCount));

			log.warn("Average size was not big enough.");
		}
		log.debug("Resizeing buffer from {} to {}", dataBuffer.limit(), newSize);
		final ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
		newBuffer.put(this.dataBuffer.array());
		this.dataBuffer = newBuffer;
	}

	/**
	 *
	 * @param candidate
	 *            the PageSearchItem to delete
	 * @throws IOException
	 */
	@Override
	public int delete(final PageSearchItem candidate) throws IOException {

		// check for duplicates.
		final SerializableTripleMap map = new SerializableTripleMap();
		final SerializableTripleFilter filter = new SerializableTripleFilter(
				candidate);
		final ExtendedIterator<TripleIndex> idxIter = tripleIndex
				.iterator(candidate);
		int count = 0;
		lock();
		try {
			while (idxIter.hasNext()) {
				final TripleIndex tripleIndex = idxIter.next();
				final SerializableTriple st = map.map1(tripleIndex);
				if (filter.accept(st)) {
					tripleIndex.delete();
					count++;
				}
			}
			flush();
			return count;
		} finally {
			unlock();
		}
	}

	/**
	 * Map a triple index to SerializableTriple instances.
	 *
	 */
	private class SerializableTripleMap implements
			Map1<TripleIndex, SerializableTriple> {
		/**
		 * Copy of the data buffer
		 */
		private final ByteBuffer buffer;

		/**
		 * constructor
		 */
		private SerializableTripleMap() {
			buffer = dataBuffer.duplicate();
		}

		@Override
		public SerializableTriple map1(final TripleIndex tripleIndex) {
			log.debug("Reading triple from offset {}", tripleIndex.getOffset());
			buffer.position(tripleIndex.getOffset());
			final int bufferLen = buffer.getInt();
			log.debug("Reading {} bytes for triple", bufferLen + Integer.BYTES);
			final ByteBuffer bb = buffer.slice();
			bb.limit(bufferLen);
			return new SerializableTriple(bb);
		}
	}

	/**
	 * Implementation of updatable page statistics.
	 *
	 */
	private class Statistics extends UpdatablePageStatistics {
		// the number of records
		private int recordCount;
		// the data size
		private int dataSize;
		// the number of deleted records.
		private int deleteCount;

		@Override
		public int getRecordCount() {
			return recordCount;
		}

		@Override
		public void incrementRecordCount() {
			recordCount++;
		}

		@Override
		public int getDataSize() {
			return dataSize;
		}

		@Override
		public void incrementDataSize(final int size) {
			dataSize += size;
		}

		@Override
		public int getDeleteCount() {
			return deleteCount;
		}

		@Override
		public void incrementDeleteCount() {
			deleteCount++;
		}

	}

}
