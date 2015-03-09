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

import java.nio.ByteBuffer;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public interface BloomCapabilities {

	/**
	 * Return true if a newly created graph is empty.
	 * 
	 * @return
	 */
	public boolean canBeEmpty();

	/**
	 * return false if the number of items in the store is not known or can only
	 * be estimated.
	 * 
	 * @return true if the value returned by getSize() is an exact count of the
	 *         number of items in the store.
	 */
	public boolean sizeAccurate();

	/**
	 * Reutrn the number of items in the store. May be an estimate if
	 * sizeAccurate() returns false.
	 * 
	 * @return the number of items in the store
	 */
	public int getSize();

	/**
	 * Return true if this store can be added to (not read only).
	 */
	public boolean addAllowed();

	/**
	 * return true if the data store will add duplicate blocks.
	 */
	public boolean addsDuplicates();

	/**
	 * Add an entry. Systems must ensure that the data block only exists once in
	 * the data store. In relational parlance the data could be considered as a
	 * primary key.
	 * 
	 * Write the entity to the store.
	 * 
	 * @param bloomValue
	 *            The bloom filter used for searching
	 * @param data
	 *            the data to write.
	 */
	void write(ByteBuffer bloomValue, ByteBuffer data);

	/**
	 * return true if this store premits deletion (not read only).
	 */
	public boolean deleteAllowed();

	/**
	 * Delete an entry. Systems must ensure that only the specified data block
	 * is removed from the store. In relational parlance the data could be
	 * considered as a primary key.
	 * 
	 * @param bloomValue
	 * @param data
	 */
	void delete(ByteBuffer bloomValue, ByteBuffer data);

	/**
	 * Find a series of entries.
	 * 
	 * @param bloomValue
	 *            the bloom value to use for searching.
	 * @param exact
	 *            if true only exact matches should be returned (if supported).
	 * @return An ExtendedIterator of the ByteBuffer values that were written to
	 *         the store.
	 */
	ExtendedIterator<ByteBuffer> find(ByteBuffer bloomValue, boolean exact);

	/**
	 * close the underlying connection to the data store
	 */
	void close();

	/**
	 * Return the largest value bit pattern for the bytebuffer
	 * 
	 * @return
	 */
	ByteBuffer getMaxBloomValue();
}
