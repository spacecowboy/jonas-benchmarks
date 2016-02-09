package org.neo4j.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

@State( Scope.Thread )
public class CreateNodes
{
    GraphDatabaseService db;
    int batchSize = 1000;

    @Setup
    public void setup() throws IOException
    {
        File dataDir = new File("/home/jonas/bahtest");
        FileUtils.deleteRecursively( dataDir );
        assert dataDir.mkdirs();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );
    }

    @Benchmark
    @BenchmarkMode( Mode.Throughput)
    @OutputTimeUnit( TimeUnit.SECONDS)
    public long[] createNodes()
    {
        long[] nodes = new long[batchSize];
        try ( Transaction tx = db.beginTx() )
        {

            for ( int i = 0; i < batchSize; i++ )
            {
                nodes[i] = db.createNode().getId();
            }

            tx.success();
        }
        return nodes;
    }
}
