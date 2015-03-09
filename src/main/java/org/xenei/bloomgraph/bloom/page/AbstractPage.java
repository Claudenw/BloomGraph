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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.bloomgraph.bloom.index.PageIndex;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * A page of triple filters.
 *
 */
public abstract class AbstractPage {

	protected final Logger log;

	// the page index for this page.
	protected PageIndex pageIndex;

	/**
	 * Constructor
	 * 
	 * @param pageIndex
	 *            The page index for this page.
	 */
	public AbstractPage(final PageIndex pageIndex) {
		this.pageIndex = pageIndex;
		// allocate enough for the entire bloom filter list
		this.log = LoggerFactory.getLogger(String.format("%s.%s",
				AbstractPage.class.getName(), pageIndex.getId()));
	}

	protected abstract void lock();

	protected abstract void unlock();

	protected abstract void flush();

	/**
	 * Get the statistics for the page.
	 * 
	 * @return The page statistics.
	 */
	final public PageStatistics getStatistics() {
		return getUpdatableStatistics();
	}

	/**
	 * get the updatable page statistics
	 * 
	 * @return The Updatable page statistics.
	 */
	protected abstract UpdatablePageStatistics getUpdatableStatistics();

	/**
	 * Find the triples that matche the candidate. Only triples that match the
	 * S,P,O of the candiate may be returned.
	 * 
	 * @param candidate
	 *            The candidate to match.
	 * @return The iterator of the matching triples.
	 * @throws IOException
	 *             on error.
	 */
	public abstract ExtendedIterator<Triple> find(final PageSearchItem candidate)
			throws IOException;

	/**
	 * Return an approximate count of the number of records that will match the
	 * candidate
	 *
	 * @param candidate
	 *            the candidate to match
	 * @return the approximate count matching candidates.
	 * @throws IOException
	 */
	public final int count(final PageSearchItem candidate) throws IOException {
		if (candidate.getTriple().equals(Triple.ANY)) {
			return getStatistics().getRecordCount()
					- getStatistics().getDeleteCount();
		}
		return doCount(candidate);

	}

	/**
	 * Get the page index.
	 * 
	 * @return The page index.
	 */
	public PageIndex getPageIndex() {
		return pageIndex;
	}

	/**
	 * Estimate the number of matching candidates.
	 * 
	 * @param candidate
	 *            The candidate to match.
	 * @return the estimate of matching triples.
	 * @throws IOException
	 */
	abstract protected int doCount(final PageSearchItem candidate)
			throws IOException;

	/**
	 * returns false if the page is full
	 *
	 * @param candidate
	 *            the PageSearchItem to write
	 * @return true if the triple is on the page, false is there is no space.
	 * @throws IOException
	 */
	final public boolean write(final PageSearchItem candidate)
			throws IOException {

		// check for duplicates.
		final ExtendedIterator<Triple> iter = find(candidate);
		try {
			if (iter.hasNext()) {
				log.debug("Already in page");
				return true;
			}
		} finally {
			iter.close();
		}
		return doWrite(candidate);
	}

	/**
	 * Write the candidate to this page.
	 * 
	 * @param candidate
	 *            The candidate to write.
	 * @return true if the candidate was written, false if there was not any
	 *         space.
	 * @throws IOException
	 *             on error other than page full.
	 */
	protected abstract boolean doWrite(final PageSearchItem candidate)
			throws IOException;

	/**
	 * Delete the candidate from this page.
	 * 
	 * @param candidate
	 *            the PageSearchItem to delete.
	 * @return the number of records deleted.
	 * @throws IOException
	 */
	abstract public int delete(final PageSearchItem candidate)
			throws IOException;

	/**
	 * Estimate of the number of triples on the page.
	 *
	 * @return estimated triple count
	 */
	public final int size() {
		return getStatistics().getRecordCount()
				- getStatistics().getDeleteCount();
	}

	/**
	 * Page debugging call. writes the statistics to the log.
	 * 
	 * @param lbl
	 *            THe label to put in the debug statement.
	 */
	public void debug(final String lbl) {
		log.debug("({}) {}", lbl, getStatistics().toString());
	}
}
