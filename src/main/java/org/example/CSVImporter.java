package org.example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CSVImporter {

    private enum RelType implements RelationshipType {
        ROAD_TO
    }

    public static void importCSV(File csvFile, GraphDatabaseService db) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;

            // Skip the first line as it contains field names
            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue; // Skip the header line
                }

                String[] tokens = line.split(",");

                // Extract basic RoadChain information (first 9 fields)
                int id = Integer.parseInt(tokens[0]);
                int gridId = Integer.parseInt(tokens[1]);
                int chainId = Integer.parseInt(tokens[2]);
                int index = Integer.parseInt(tokens[3]);
                int length = Integer.parseInt(tokens[4]);
                int level = Integer.parseInt(tokens[5]);
                int inCount = Integer.parseInt(tokens[6]);
                int outCount = Integer.parseInt(tokens[7]);
                int direction = Integer.parseInt(tokens[8]);

                List<RoadConnection> in_connections = new ArrayList<>();
                List<RoadConnection> out_connections = new ArrayList<>();

                // Extract in-connections and out-connections (5-tuple information)
                int currentIndex = 9;
                for (int i = 0; i < inCount; i++) {
                    RoadConnection inConnection = parseRoadConnection(tokens, currentIndex);
                    in_connections.add(inConnection);
                    currentIndex += 5;
                }
                for (int i = 0; i < outCount; i++) {
                    RoadConnection outConnection = parseRoadConnection(tokens, currentIndex);
                    out_connections.add(outConnection);
                    currentIndex += 5;
                }

                RoadChain roadChain = new RoadChain(id, gridId, chainId, index, length, level, inCount, outCount, direction, in_connections, out_connections);

                // Add the RoadChain and its connections to the database
                try (Transaction tx = db.beginTx()) {
                    Node roadChainNode = tx.createNode(Label.label("RoadChain"));
                    roadChainNode.setProperty("id", roadChain.getId());
                    roadChainNode.setProperty("gridId", roadChain.getGridId());
                    roadChainNode.setProperty("chainId", roadChain.getChainId());
                    roadChainNode.setProperty("index", roadChain.getIndex());
                    roadChainNode.setProperty("length", roadChain.getLength());
                    roadChainNode.setProperty("level", roadChain.getLevel());
                    roadChainNode.setProperty("inCount", roadChain.getInCount());
                    roadChainNode.setProperty("outCount", roadChain.getOutCount());
                    roadChainNode.setProperty("direction", roadChain.getDirection());
                    roadChainNode.setProperty("in_connections", roadChain.getInConnections());
                    roadChainNode.setProperty("out_connections", roadChain.getOutConnections());

                    // Create relationships for in-connections
                    for (RoadConnection inConnection : roadChain.getInConnections()) {
                        Node inNode = findOrCreateNode(db, inConnection);
                        Relationship relationship = inNode.createRelationshipTo(roadChainNode, RelType.ROAD_TO);
                        setConnectionProperties(relationship, inConnection);
                    }

                    // Create relationships for out-connections
                    for (RoadConnection outConnection : roadChain.getOutConnections()) {
                        Node outNode = findOrCreateNode(db, outConnection);
                        Relationship relationship = roadChainNode.createRelationshipTo(outNode, RelType.ROAD_TO);
                        setConnectionProperties(relationship, outConnection);
                    }

                    tx.commit();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static RoadConnection parseRoadConnection(String[] tokens, int startIndex) {
        int gridId = Integer.parseInt(tokens[startIndex]);
        int chainId = Integer.parseInt(tokens[startIndex + 1]);
        int index = Integer.parseInt(tokens[startIndex + 2]);
        int length = Integer.parseInt(tokens[startIndex + 3]);
        int direction = Integer.parseInt(tokens[startIndex + 4]);
        return new RoadConnection(gridId, chainId, index, length, direction);
    }

    private static Node findOrCreateNode(GraphDatabaseService db, RoadConnection connection) {
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.findNode(Label.label("RoadChain"), "gridId", connection.getGridId(), "chainId", connection.getChainId());
            if (node == null) {
                node = tx.createNode(Label.label("RoadChain"));
                node.setProperty("gridId", connection.getGridId());
                node.setProperty("chainId", connection.getChainId());
            }
            tx.commit();
        }
        return node;
    }

    private static void setConnectionProperties(Relationship relationship, RoadConnection connection) {
        relationship.setProperty("index", connection.getIndex());
        relationship.setProperty("length", connection.getLength());
        relationship.setProperty("direction", connection.getDirection());
    }

    public static void main(final String[] args) throws IOException {
        File csvFile = new File("topo.csv");
        neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}