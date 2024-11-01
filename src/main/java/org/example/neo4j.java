/*
 * Licensed to Neo4j under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo4j licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.example;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

public class neo4j {
    private static final Path databaseDirectory = Path.of("neo4j");

    GraphDatabaseService graphDb;

    private DatabaseManagementService managementService;

    private enum RelTypes implements RelationshipType {
        KNOWS
    }

    /*
    public static void main(final String[] args) throws IOException {
        EmbeddedNeo4j hello = new EmbeddedNeo4j();
        hello.createDb();
        hello.removeData();
        hello.shutDown();
    }
    */

    void createDb() throws IOException {
        if (Files.exists(databaseDirectory)) {
            FileUtils.deleteDirectory(databaseDirectory);
        }

        // tag::startDb[]
        managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
        graphDb = managementService.database(DEFAULT_DATABASE_NAME);
        System.out.println("Database is running ...");
        registerShutdownHook(managementService);
        // end::startDb[]

    }


    void shutDown() {
        System.out.println();
        System.out.println("Shutting down database ...");
        // tag::shutdownServer[]
        managementService.shutdown();
        // end::shutdownServer[]
    }

    // tag::shutdownHook[]
    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                managementService.shutdown();
            }
        });
    }
    // end::shutdownHook[]
}
