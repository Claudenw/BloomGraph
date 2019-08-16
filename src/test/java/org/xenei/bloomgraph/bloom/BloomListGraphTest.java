package org.xenei.bloomgraph.bloom;

import org.xenei.bloomfilter.FilterConfig;
import org.xenei.bloomfilter.collections.BloomCollection;
import org.xenei.bloomfilter.collections.BloomList;
import org.xenei.bloomgraph.BloomTriple;

public class BloomListGraphTest extends AbstractBloomGraphTest {

	@Override
	protected BloomCollection<BloomTriple> makeCollection() {
		FilterConfig filterConfig = new FilterConfig(10, 10);

		return 	new BloomList<BloomTriple>(filterConfig, BloomTriple.FUNC);
	}

}
