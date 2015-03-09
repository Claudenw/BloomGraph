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
 * The page statistics
 *
 */
public interface PageStatistics {
	/**
	 * The number of triples on the page.
	 * 
	 * @return number of records
	 */
	public int getRecordCount();

	/**
	 * Bytes of data consumed by the triples.
	 * 
	 * @return bytes of data.
	 */
	public int getDataSize();

	/**
	 * The number of deleted triples
	 * 
	 * @return The number of deleted records
	 */
	public int getDeleteCount();

	/**
	 * A value between [0 and 1] indicating the percentage of records that have
	 * not been deleted.
	 * 
	 * @return density.
	 */
	public double getDensity();

}
