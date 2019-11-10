package org.xenei.bloomgraph.bloom;

import org.junit.runner.RunWith;
import org.xenei.bloom.multidimensional.ContainerImpl;
import org.xenei.bloom.multidimensional.Container;
import org.xenei.bloom.multidimensional.Container.Index;
import org.xenei.bloom.multidimensional.Container.Storage;
import org.xenei.bloom.multidimensional.index.FlatBloofi;
import org.xenei.bloom.multidimensional.storage.InMemory;
import org.xenei.bloomgraph.BloomTriple;
import org.xenei.junit.contract.Contract;
import org.xenei.junit.contract.ContractImpl;
import org.xenei.junit.contract.ContractSuite;
import org.xenei.junit.contract.IProducer;
import org.apache.commons.collections4.bloomfilter.BloomFilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.Murmur128;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.testing_framework.AbstractGraphProducer;

@RunWith(ContractSuite.class)
@ContractImpl(BloomGraph.class)
public class BloomGraph_CS_Test  {

    private Shape shape = new Shape( Murmur128.NAME, 3, 1.0/30000000 );



        private IProducer<BloomGraph> producer = new AbstractGraphProducer<BloomGraph>() {

                @Override
                public BloomGraph createNewGraph() {
                    Storage<BloomTriple> storage = new InMemory<BloomTriple>();
                    Index index = new FlatBloofi( shape );
                    Container<BloomTriple> container = new ContainerImpl<BloomTriple>( shape, storage, index );
                    return new BloomGraph(container);
                }

                @Override
                public Graph[] getDependsOn(Graph g) {
                    return null;
                }

                @Override
                public Graph[] getNotDependsOn(Graph g) {
                    return new Graph[] { new GraphMem() };
                }

        };

        @Contract.Inject
        public IProducer<BloomGraph> getTripleStore() {
                return producer;
        }
}
