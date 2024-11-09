package org.example;

import javafx.util.Pair;
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
        Node crossNodesStart = null;
        Node crossNodesEnd = null;
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

                Map<Pair<Integer, Integer>, Pair<Integer, Integer>> map = new HashMap<>(); // 存储路链（grid, chain）->起始和终止节点(startIndex, endIndex)
                int startNode = 0, endNode = 0;

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
                    if(map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                        hasConnectIn = true;
                    }
                }
                for (int i = 0; i < outCount; i++) {
                    RoadConnection outConnection = parseRoadConnection(tokens, currentIndex);
                    out_connections.add(outConnection);
                    currentIndex += 5;
                    if(map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                        hasConnectOut = true;
                    }
                }

                // create 主链
                if(map.containsKey(new Pair<>(gridId, chainId))) { // 如果主链在map中，则忽略
                    continue;
                }

                if((!hasConnectIn) && (!hasConnectOut)) {
                    try (Transaction tx = db.beginTx()) {
                        crossNodesStart = tx.createNode(Label.label("crossNode"));
                        startNode = nodeindex;
                        crossNodesStart.setProperty("index", nodeindex++);
                        crossNodesEnd = tx.createNode(Label.label("crossNode"));
                        endNode = nodeindex;
                        crossNodesEnd.setProperty("index", nodeindex++);
                        tx.commit();
                    }
                }
                else if(hasConnectIn && !hasConnectOut) {
                    for(RoadConnection inConnection : in_connections) {
                        if(map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                            startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                            try (Transaction tx = db.beginTx()) {
                                crossNodesStart = tx.findNode(Label.label("crossNode"), "index", startNode);
                                endNode = nodeindex;
                                crossNodesEnd = tx.createNode(Label.label("crossNode"));
                                crossNodesEnd.setProperty("index", nodeindex++);
                                tx.commit();
                            }
                        }
                    }
                }
                else if(!hasConnectIn && hasConnectOut) {
                    for(RoadConnection outConnection : out_connections) {
                        if(map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                            endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getKey();
                            try (Transaction tx = db.beginTx()) {
                                crossNodesEnd = tx.findNode(Label.label("crossNode"), "index", endNode);
                                startNode = nodeindex;
                                crossNodesStart = tx.createNode(Label.label("crossNode"));
                                crossNodesStart.setProperty("index", nodeindex++);
                                tx.commit();
                            }
                        }
                    }
                }
                else {
                    for(RoadConnection inConnection : in_connections) {
                        if(map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                            startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                            try (Transaction tx = db.beginTx()) {
                                crossNodesStart = tx.findNode(Label.label("crossNode"), "index", startNode);
                                tx.commit();
                            }
                            break;
                        }
                    }
                    for(RoadConnection outConnection : out_connections) {
                        if(map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                            endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getKey();
                            try (Transaction tx = db.beginTx()) {
                                crossNodesEnd = tx.findNode(Label.label("crossNode"), "index", endNode);
                                tx.commit();
                            }
                            break;
                        }
                    }
                }
                try (Transaction tx = db.beginTx()) {
                    map.put(new Pair<>(gridId, chainId), new Pair<>(startNode, endNode));
                    Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                    road.setProperty("gridId", gridId);
                    road.setProperty("chainId", chainId);
                    tx.commit();
                }

                // 添加入链（对 主链来说）
                for(RoadConnection inConnection : in_connections) {
                    if(map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                        continue;
                    }
                    try (Transaction tx = db.beginTx()) {
                        Node crossNodesStartIn = tx.createNode(Label.label("crossNode")); // 左边创一个节点
                        int startNodeIn = nodeindex;
                        crossNodesStartIn.setProperty("index", nodeindex++);
                        //Node crossNodesEndIn = tx.findNode(Label.label("crossNode"), "index", startNode);
                        Relationship road = crossNodesStartIn.createRelationshipTo(crossNodesStart, RelType.ROAD_TO);
                        road.setProperty("gridId", inConnection.getGridId());
                        road.setProperty("chainId", inConnection.getChainId());
                        map.put(new Pair<>(inConnection.getGridId(), inConnection.getChainId()), new Pair<>(startNodeIn, startNode));
                        tx.commit();
                    }
                }
                // 添加出链（对 主链来说）
                for(RoadConnection outConnection : out_connections) {
                    if(map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                        continue;
                    }
                    try (Transaction tx = db.beginTx()) {
                        Node crossNodesEndOut = tx.createNode(Label.label("crossNode")); // 右边创一个节点
                        int endNodeOut = nodeindex;
                        crossNodesEndOut.setProperty("index", nodeindex++);
                        //Node crossNodesStartOut = tx.findNode(Label.label("crossNode"), "index", endNode);
                        Relationship road = crossNodesEnd.createRelationshipTo(crossNodesEndOut, RelType.ROAD_TO);
                        road.setProperty("gridId", outConnection.getGridId());
                        road.setProperty("chainId", outConnection.getChainId());
                        map.put(new Pair<>(outConnection.getGridId(), outConnection.getChainId()), new Pair<>(endNode, endNodeOut));
                        tx.commit();
                    }
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


    public static void main(final String[] args) throws IOException {
        File csvFile = new File("D:\\Desktop\\study\\buaa\\neo4j_1\\src\\main\\java\\org\\example\\Topo.csv");
        neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}