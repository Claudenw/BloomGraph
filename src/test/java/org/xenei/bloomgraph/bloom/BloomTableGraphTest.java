package org.xenei.bloomgraph.bloom;

import org.xenei.bloomfilter.collections.BloomCollection;
import org.xenei.bloomfilter.collections.BloomTable;
import org.xenei.bloomgraph.BloomTriple;

public class BloomTableGraphTest extends AbstractBloomGraphTest {

	@Override
	protected BloomCollection<BloomTriple> makeCollection() {
		return new BloomTable<BloomTriple>(BloomTriple.FUNC);
	}

}
