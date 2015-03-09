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

/**
 * The updatable page statistics.
 * 
 * Page statistics and the ability to update them.
 *
 */
public abstract class UpdatablePageStatistics implements PageStatistics {

	/**
	 * Incerment the number of records
	 */
	public abstract void incrementRecordCount();

	/**
	 * Increment the data cound by size bytes.
	 * 
	 * @param size
	 *            The size to increment the size by.
	 */
	public abstract void incrementDataSize(int size);

	/**
	 * Increment the number of deleted records.
	 */
	public abstract void incrementDeleteCount();

	@Override
	public String toString() {
		return String.format("Records: %s Data size: %s Deleted: %s",
				getRecordCount(), getDataSize(), getDeleteCount());
	}

	@Override
	public final double getDensity() {
		if (getRecordCount() == 0) {
			return 0;
		}
		final double count = (1.0 * getRecordCount()) - getDeleteCount();
		return count / getRecordCount();
	}
}
