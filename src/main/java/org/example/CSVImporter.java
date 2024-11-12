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

    static Map<Pair<Integer, Integer>, Pair<Integer, Integer>> map = new HashMap<>(); // 存储路链（grid, chain）-> 起始和终止节点(startIndex, endIndex)

    public static void importCSV(File csvFile, GraphDatabaseService db) {
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
                    boolean hasConnectIn = in_connections.stream().anyMatch(
                            inConn -> map.containsKey(new Pair<>(inConn.getGridId(), inConn.getChainId())));
                    boolean hasConnectOut = out_connections.stream().anyMatch(
                            outConn -> map.containsKey(new Pair<>(outConn.getGridId(), outConn.getChainId())));

                    if(hasConnectIn) { // 查找是否有入链连接
                        for(RoadConnection inConnection : in_connections) {
                            if (map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                                startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                                crossNodesStart = tx.findNode(Label.label("CrossNode"), "id", startNode);
                                break;
                            }
                        }
                    }
                    else { // 没有连接的话，新建一个节点
                        crossNodesStart = tx.createNode(Label.label("CrossNode"));
                        startNode = nodeindex++;
                        crossNodesStart.setProperty("id", startNode);
                    }

                    if(hasConnectOut) { // 查找是否有出链连接
                        for(RoadConnection outConnection : out_connections) {
                            if (map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                                endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getValue();
                                crossNodesEnd = tx.findNode(Label.label("CrossNode"), "id", endNode);
                                break;
                            }
                        }
                    }
                    else { // 没有连接的话，新建一个节点
                        crossNodesEnd = tx.createNode(Label.label("CrossNode"));
                        endNode = nodeindex++;
                        crossNodesEnd.setProperty("id", endNode);
                    }
                    map.put(new Pair<>(gridId, chainId), new Pair<>(startNode, endNode));
                    Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                    //road.setProperty("id", id);
                    road.setProperty("gridId", gridId);
                    road.setProperty("chainId", chainId);
                    tx.commit();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void importDyRoadInfo (File DyRoadInfoFile, GraphDatabaseService db) {
        try (BufferedReader br = new BufferedReader(new FileReader(DyRoadInfoFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Split the line by spaces
                String[] parts = line.split(" ");
                // Split the second part by underscore to get two fields
                String[] subParts = parts[1].split("_");
                // Parse each part into an int and remove leading zeros
                int time = Integer.parseInt(parts[0]);
                int gridId = Integer.parseInt(subParts[0]);
                int chainId = Integer.parseInt(subParts[1]);
                int travelTime = Integer.parseInt(parts[2]);
                int congestionLevel = Integer.parseInt(parts[3]); // 拥堵程度
                int numberOfVehicles = Integer.parseInt(parts[4]); // 链路车辆数

                try (Transaction tx = db.beginTx()) {
                    int startIndex = map.get(new Pair<>(gridId, chainId)).getKey();
                    int endIndex = map.get(new Pair<>(gridId, chainId)).getValue();
                    Node startNode = tx.findNode(Label.label("CrossNode"), "id", startIndex);
                    Node endNode = tx.findNode(Label.label("CrossNode"), "id", endIndex);
                    Relationship road = startNode.createRelationshipTo(endNode, RelType.ROAD_TO);
                    road.setProperty("time", time);
                    road.setProperty("gridId", gridId);
                    road.setProperty("chainId", chainId);
                    road.setProperty("travelTime", travelTime);
                    road.setProperty("congestionLevel", congestionLevel);
                    road.setProperty("numberOfVehicles", numberOfVehicles);
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
        File csvFile = new File("src\\main\\java\\org\\example\\Topo.csv");
        File DyRoadInfoFile = new File("src\\main\\java\\org\\example\\100501.csv");
        neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        //importDyRoadInfo(DyRoadInfoFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}