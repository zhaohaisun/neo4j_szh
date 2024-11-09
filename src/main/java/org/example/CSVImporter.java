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
        Map<Pair<Integer, Integer>, Pair<Integer, Integer>> map = new HashMap<>(); // 存储路链（grid, chain）-> 起始和终止节点(startIndex, endIndex)
        int nodeindex = 0;

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
                int inCount = Integer.parseInt(tokens[6]);
                int outCount = Integer.parseInt(tokens[7]);

                // 遍历所有的in_connections和out_connections，找是否和外面有连接
                List<RoadConnection> in_connections = new ArrayList<>();
                List<RoadConnection> out_connections = new ArrayList<>();
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

                try (Transaction tx = db.beginTx()) {
                    Node crossNodesStart = null;
                    Node crossNodesEnd = null;
                    int startNode = 0, endNode = 0;

                    // 主链的起始节点与结束节点创建
                    if (map.containsKey(new Pair<>(gridId, chainId))) { // 如果主链在map中，则忽略
                        continue;
                    }

                    boolean hasConnectIn = in_connections.stream().anyMatch(
                            inConn -> map.containsKey(new Pair<>(inConn.getGridId(), inConn.getChainId())));
                    boolean hasConnectOut = out_connections.stream().anyMatch(
                            outConn -> map.containsKey(new Pair<>(outConn.getGridId(), outConn.getChainId())));

                    if (!hasConnectIn && !hasConnectOut) {
                        crossNodesStart = tx.createNode(Label.label("crossNode"));
                        crossNodesStart.setProperty("index", nodeindex++);
                        startNode = (int) crossNodesStart.getProperty("index");

                        crossNodesEnd = tx.createNode(Label.label("crossNode"));
                        crossNodesEnd.setProperty("index", nodeindex++);
                        endNode = (int) crossNodesEnd.getProperty("index");
                    } else if (hasConnectIn && !hasConnectOut) {
                        for (RoadConnection inConnection : in_connections) {
                            if (map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                                startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                                crossNodesStart = tx.findNode(Label.label("crossNode"), "index", startNode);
                                crossNodesEnd = tx.createNode(Label.label("crossNode"));
                                crossNodesEnd.setProperty("index", nodeindex++);
                                endNode = (int) crossNodesEnd.getProperty("index");
                                break;
                            }
                        }
                    } else if (!hasConnectIn && hasConnectOut) {
                        for (RoadConnection outConnection : out_connections) {
                            if (map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                                endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getKey();
                                crossNodesEnd = tx.findNode(Label.label("crossNode"), "index", endNode);
                                crossNodesStart = tx.createNode(Label.label("crossNode"));
                                crossNodesStart.setProperty("index", nodeindex++);
                                startNode = (int) crossNodesStart.getProperty("index");
                                break;
                            }
                        }
                    } else {
                        for (RoadConnection inConnection : in_connections) {
                            if (map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                                startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                                crossNodesStart = tx.findNode(Label.label("crossNode"), "index", startNode);
                                break;
                            }
                        }
                        for (RoadConnection outConnection : out_connections) {
                            if (map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                                endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getKey();
                                crossNodesEnd = tx.findNode(Label.label("crossNode"), "index", endNode);
                                break;
                            }
                        }
                    }

                    // 创建主链的关系
                    if (crossNodesStart != null && crossNodesEnd != null) {
                        Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                        road.setProperty("gridId", gridId);
                        road.setProperty("chainId", chainId);
                        map.put(new Pair<>(gridId, chainId), new Pair<>(startNode, endNode));
                    }

                    // 添加入链（对 主链来说）
                    for (RoadConnection inConnection : in_connections) {
                        if (map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                            continue;
                        }
                        Node crossNodesStartIn = tx.createNode(Label.label("crossNode"));
                        crossNodesStartIn.setProperty("index", nodeindex++);
                        Relationship road = crossNodesStartIn.createRelationshipTo(crossNodesStart, RelType.ROAD_TO);
                        road.setProperty("gridId", inConnection.getGridId());
                        road.setProperty("chainId", inConnection.getChainId());
                        map.put(new Pair<>(inConnection.getGridId(), inConnection.getChainId()),
                                new Pair<>((int) crossNodesStartIn.getProperty("index"), startNode));
                    }

                    // 添加出链（对 主链来说）
                    for (RoadConnection outConnection : out_connections) {
                        if (map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                            continue;
                        }
                        Node crossNodesEndOut = tx.createNode(Label.label("crossNode"));
                        crossNodesEndOut.setProperty("index", nodeindex++);
                        Relationship road = crossNodesEnd.createRelationshipTo(crossNodesEndOut, RelType.ROAD_TO);
                        road.setProperty("gridId", outConnection.getGridId());
                        road.setProperty("chainId", outConnection.getChainId());
                        map.put(new Pair<>(outConnection.getGridId(), outConnection.getChainId()),
                                new Pair<>(endNode, (int) crossNodesEndOut.getProperty("index")));
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


    public static void main(final String[] args) throws IOException {
        File csvFile = new File("D:\\Desktop\\study\\buaa\\neo4j_1\\src\\main\\java\\org\\example\\Topo.csv");
        neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}