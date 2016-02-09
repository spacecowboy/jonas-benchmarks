package org.neo4j.benchmarks;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

@State( Scope.Thread )
public class TransactionStateNoState
{
    GraphDatabaseService db;
    int numNodes = 25_000;
    int numReads = 1_000_000;
    int numProps = 10;
    Node[] nodes;
    String[] propNames;

    @Setup
    public void setup() throws IOException
    {
        File dataDir = new File("/home/jonas/bahtest");
        FileUtils.deleteRecursively( dataDir );
        assert dataDir.mkdirs();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );

        propNames = new String[numProps];
        for ( int propNum = 0; propNum < numProps; propNum++ )
        {
            propNames[propNum] = "key_" + propNum;
        }

        nodes = new Node[numNodes];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numNodes; i++ )
            {
                nodes[i] = db.createNode();

                for ( int propNum = 0; propNum < numProps; propNum++ )
                {
                    nodes[i].setProperty( propNames[propNum], propNum );
                }
            }
            tx.success();
        }
    }

    @TearDown
    public void teardown()
    {
    }

    @Benchmark
    @BenchmarkMode( Mode.Throughput )
    @OperationsPerInvocation(1_000_000)
    @OutputTimeUnit( TimeUnit.SECONDS )
    public int readWithoutTxState()
    {
        int result = 0;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < numReads; i++ )
            {
                Node node = nodes[i % nodes.length];
                Map<String,Object> props = node.getProperties( propNames );
                // JMH guard
                if ( props.containsKey( "key_1" ) )
                {
                    result++;
                }
            }
            tx.success();
        }
        return result;
    }
}
