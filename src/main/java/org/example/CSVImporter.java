package org.example;

import org.antlr.v4.runtime.misc.Pair;
import org.neo4j.cypher.internal.expressions.In;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class CSVImporter {

    private enum RelType implements RelationshipType {
        ROAD_TO
    }

    public static void importCSV(File csvFile, GraphDatabaseService db) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;
            int nodeindex = 0;

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

                //Set<Pair<Integer, Integer>> usedChains = new HashSet<>(); // 存储分配过的路链
                //String nodeIndex = gridId + "_" + chainId; // 为每个节点分配唯一的索引
                Map<Pair<Integer, Integer>, Pair<Integer, Integer>> map = new HashMap<>(); // 存储路链->起始和终止节点
                int startNode, endNode;
                Node crossNodesStart;
                Node crossNodesEnd;
                try (Transaction tx = db.beginTx()) {
                    if(map.containsKey(new Pair<>(gridId, chainId))) {
                        //(gridId, chainId)key对应的值(startNode, endNode)
                        Pair<Integer, Integer> retrieved = map.get(new Pair<>(gridId, chainId));
                        startNode = retrieved.a;
                        endNode = retrieved.b;
                        crossNodesStart = tx.findNode(Label.label("crossNode"), "index", startNode); // 标签为 `crossNode`，并且其属性 `index` 的值等于 `nodeIndex` 的节点
                        crossNodesEnd = tx.findNode(Label.label("crossNode"), "index", endNode);
                    } else {
                        crossNodesStart = tx.createNode(Label.label("crossNode"));
                        startNode = nodeindex;
                        crossNodesStart.setProperty("index", nodeindex++);
                        crossNodesEnd = tx.createNode(Label.label("crossNode"));
                        endNode = nodeindex;
                        crossNodesEnd.setProperty("index", nodeindex++);
                        map.put(new Pair<>(gridId, chainId), new Pair<>(startNode, endNode));
                        Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                        //road.setProperty("id", id);
                        road.setProperty("gridId", gridId);
                        road.setProperty("chainId", chainId);
                        //road.setProperty("index", index);
                        //road.setProperty("length", length);
                        //road.setProperty("level", level);
                        //road.setProperty("inCount", inCount);
                        //road.setProperty("outCount", outCount);
                        //road.setProperty("direction", direction);
                    }
                    tx.commit();
                }

                /* Add the RoadChain and its connections to the database
                try (Transaction tx = db.beginTx()) {
                    Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                    //road.setProperty("id", id);
                    road.setProperty("gridId", gridId);
                    road.setProperty("chainId", chainId);
                    //road.setProperty("index", index);
                    //road.setProperty("length", length);
                    road.setProperty("level", level);
                    road.setProperty("inCount", inCount);
                    road.setProperty("outCount", outCount);
                    //road.setProperty("direction", direction);
                    tx.commit();
                }*/

                // 遍历所有的in_connections和out_connections，找是否和外面有连接
                List<RoadConnection> in_connections = new ArrayList<>();
                List<RoadConnection> out_connections = new ArrayList<>();

                boolean hasConnectIn = false, hasConnectOut = false;

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

    private static Node findOrCreateNode(GraphDatabaseService db, int index) {
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.findNode(Label.label("crossNode"), "index", index);
            if (node == null) {
                node = tx.createNode(Label.label("crossNode"));
                node.setProperty("index", index);
            }
            tx.commit();
        }
        return node;
    }


    public static void main(final String[] args) throws IOException {
        File csvFile = new File("D:\\Desktop\\study\\buaa\\neo4j_1\\src\\main\\java\\org\\example\\Topo.csv");
        neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}