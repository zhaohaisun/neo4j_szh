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
import org.neo4j.graphdb.schema.Schema;
import scala.Int;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class CSVImporter {
    static neo4j neo4j_Bj = new neo4j();

    private enum RelType implements RelationshipType {
        ROAD_TO
    }

    static Map<Pair<Integer, Integer>, Pair<Integer, Integer>> map = new HashMap<>(); // 存储路链（grid, chain）-> 起始和终止节点(startIndex, endIndex)
    //static Map<Integer, Node> nodeMap = new HashMap<>(); // 存储节点id -> 节点

    final static int numOfTransactions = 5;

    public static void importCSV(File csvFile, GraphDatabaseService db) {
        int nodeindex = 0;
        int cnt = 0;
        try (Transaction tx = db.beginTx()) {
            Schema schema = tx.schema();
            // 为 CrossNode 标签的 id 属性创建唯一约束
            schema.constraintFor(Label.label("CrossNode"))
                    .assertPropertyIsUnique("id")
                    .create();
            tx.commit();
            System.out.println("Unique constraint on 'id' created successfully.");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean isFirstLine = true;

            // 启动第一个事务
            Transaction tx = db.beginTx();
            // 跳过文件的第一行，第一行是字段名
            while ((line = br.readLine()) != null) {
                cnt++;
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;  // 跳过表头
                }

                String[] tokens = line.split(",");

                // 提取基础的道路链信息
                int id = Integer.parseInt(tokens[0]);
                int gridId = Integer.parseInt(tokens[1]);
                int chainId = Integer.parseInt(tokens[2]);
                int inCount = Integer.parseInt(tokens[6]);
                int outCount = Integer.parseInt(tokens[7]);

                // 解析 in 和 out 连接
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

                Node crossNodesStart = null;
                Node crossNodesEnd = null;
                int startNode = 0, endNode = 0;

                boolean hasConnectIn = in_connections.stream().anyMatch(
                        inConn -> map.containsKey(new Pair<>(inConn.getGridId(), inConn.getChainId())));
                boolean hasConnectOut = out_connections.stream().anyMatch(
                        outConn -> map.containsKey(new Pair<>(outConn.getGridId(), outConn.getChainId())));

                if (hasConnectIn) {
                    for (RoadConnection inConnection : in_connections) {
                        if (map.containsKey(new Pair<>(inConnection.getGridId(), inConnection.getChainId()))) {
                            startNode = map.get(new Pair<>(inConnection.getGridId(), inConnection.getChainId())).getValue();
                            crossNodesStart = tx.findNode(Label.label("CrossNode"), "id", startNode);
                            break;
                        }
                    }
                } else {
                    crossNodesStart = tx.createNode(Label.label("CrossNode"));
                    startNode = nodeindex++;
                    crossNodesStart.setProperty("id", startNode);
                }

                if (hasConnectOut) {
                    for (RoadConnection outConnection : out_connections) {
                        if (map.containsKey(new Pair<>(outConnection.getGridId(), outConnection.getChainId()))) {
                            endNode = map.get(new Pair<>(outConnection.getGridId(), outConnection.getChainId())).getValue();
                            crossNodesEnd = tx.findNode(Label.label("CrossNode"), "id", endNode);
                            break;
                        }
                    }
                } else {
                    crossNodesEnd = tx.createNode(Label.label("CrossNode"));
                    endNode = nodeindex++;
                    crossNodesEnd.setProperty("id", endNode);
                }

                map.put(new Pair<>(gridId, chainId), new Pair<>(startNode, endNode));

                // 创建关系
                Relationship road = crossNodesStart.createRelationshipTo(crossNodesEnd, RelType.ROAD_TO);
                road.setProperty("gridId", gridId);
                road.setProperty("chainId", chainId);

                // 批量提交逻辑
                if (cnt % numOfTransactions == 0) {  // 每numOfTransactions个记录提交一次
                    tx.commit();

                    // 提交后重新开始一个新的事务
                    tx = db.beginTx();
                }
            }
            // 提交剩余的数据
            tx.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Imported Topo.csv to Neo4j");
    }

    public static void importDyRoadInfo (File DyRoadInfoFile, GraphDatabaseService db) {
        try (BufferedReader br = new BufferedReader(new FileReader(DyRoadInfoFile))) {
            Map<Integer, Node> nodeCache = new HashMap<>();
            String line;
            Transaction tx = db.beginTx(); // 开始事务
            int numOfTransactions = 1000;
            int cnt = 0;

            while ((line = br.readLine()) != null) {
                cnt++;
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

                int startIndex = map.get(new Pair<>(gridId, chainId)).getKey();
                int endIndex = map.get(new Pair<>(gridId, chainId)).getValue();

                //Node startNode = tx.findNode(Label.label("CrossNode"), "id", startIndex);
                //Node endNode = tx.findNode(Label.label("CrossNode"), "id", endIndex);
                Node startNode, endNode;

                if(nodeCache.containsKey(startIndex)) {
                    startNode = nodeCache.get(startIndex);
                } else {
                    startNode = tx.findNode(Label.label("CrossNode"), "id", startIndex);
                    nodeCache.put(startIndex, startNode);
                }

                if(nodeCache.containsKey(endIndex)) {
                    endNode = nodeCache.get(endIndex);
                } else {
                    endNode = tx.findNode(Label.label("CrossNode"), "id", endIndex);
                    nodeCache.put(endIndex, endNode);
                }

                Relationship road = startNode.createRelationshipTo(endNode, RelType.ROAD_TO);
                road.setProperty("time", time);
                road.setProperty("gridId", gridId);
                road.setProperty("chainId", chainId);
                road.setProperty("travelTime", travelTime);
                road.setProperty("congestionLevel", congestionLevel);
                road.setProperty("numberOfVehicles", numberOfVehicles);

                if (cnt % numOfTransactions == 0) {  // 每numOfTransactions个记录提交一次
                    tx.commit();
                    nodeCache.clear();
                    tx = db.beginTx(); // 提交后重新开始一个新的事务
                }
            }
            tx.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Imported 100501.csv to Neo4j");
    }

    private static RoadConnection parseRoadConnection(String[] tokens, int startIndex) {
        int gridId = Integer.parseInt(tokens[startIndex]);
        int chainId = Integer.parseInt(tokens[startIndex + 1]);
        int index = Integer.parseInt(tokens[startIndex + 2]);
        int length = Integer.parseInt(tokens[startIndex + 3]);
        int direction = Integer.parseInt(tokens[startIndex + 4]);
        return new RoadConnection(gridId, chainId, index, length, direction);
    }
}