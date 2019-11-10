package org.xenei.bloomgraph.bloom;


import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloomgraph.BloomTriple;

public class BloomListGraphTest extends AbstractBloomGraphTest {

	@Override
	protected Container<BloomTriple> makeCollection() {
	    Shape shape = new Shape( Murmur128.NAME, 3, 1.0/30000000 );
        Storage<BloomTriple> storage = new InMemory<BloomTriple>();
        Index index = new FlatBloofi( shape );
        return new ContainerImpl<BloomTriple>( shape, storage, index );
	}

}
