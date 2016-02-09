/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;

@State( Scope.Thread )
public class CreateNodesWithLabel
{

    GraphDatabaseService db;
    int batchSize = 1000;
    Label label;

    @Setup
    public void setup() throws IOException
    {
        label = DynamicLabel.label( "TestLabel" );
        File dataDir = new File("/home/jonas/bahtest");
        FileUtils.deleteRecursively( dataDir );
        assert dataDir.mkdirs();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dataDir );
    }

    @TearDown
    public void teardown()
    {

    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit( TimeUnit.SECONDS)
    public long[] createNodesWithLabel()
    {
        long[] nodes = new long[batchSize];
        try ( Transaction tx = db.beginTx() )
        {

            for ( int i = 0; i < batchSize; i++ )
            {
                nodes[i] = db.createNode( label ).getId();
            }

            tx.success();
        }
        return nodes;
    }
}
