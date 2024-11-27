package org.example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;

import static org.example.CSVImporter.neo4j_Bj;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class HistoricalSnapshotQuery {
    /* 历史快照查询
     * snapshot(TG, tp, t) -> <(entity_1, value_1), ..., (entity_n, value_n)>
    public Map<String, Object> snapshot(String tp, int time) {
     * tp: 查询的时态属性
     * t: 时间点
     * 返回: (entity, value)组成的集合，entity表示点/边，value为该点/边在时间t的tp属性值
     */

    public Map<String, Integer> snapshot(GraphDatabaseService graphDb, String tp, int time) {
        System.out.println("Start querying historical snapshot ...");
        Map<String, Integer> result = new HashMap<>();
        try (Transaction tx = graphDb.beginTx()) {
            // 获取所有关系并过滤
            for (Relationship relationship : tx.getAllRelationships()) {
                if (relationship.getType().name().equals("ROAD_TO")) {
                    // 检查时间属性
                    int relTime = (int)relationship.getProperty("time", 0); // 默认值为""
                    if (relTime == time) {
                        // 获取起始和终止节点
                        Node startNode = relationship.getStartNode();
                        Node endNode = relationship.getEndNode();
                        
                        // 构建实体标识符
                        String entity = startNode.getProperty("id") + "->" + endNode.getProperty("id");
                        
                        // 获取指定属性值
                        if (relationship.hasProperty(tp)) {
                            int value = (int)relationship.getProperty(tp);
                            result.put(entity, value);
                        }
                    }
                }
            }
            tx.commit();
        }
        return result;
    }

    // 使用
    public static void main(String[] args) {
        neo4j_Bj.startDb();
        HistoricalSnapshotQuery query = new HistoricalSnapshotQuery();
        System.out.println("Please input the time property and the time in two line: ");
        Scanner scanner = new Scanner(System.in);
        String tpInput = scanner.nextLine();
        String tInput = scanner.nextLine();
        int tint = Integer.parseInt(tInput);
        System.out.println("Querying historical snapshot ...");
        Map<String, Integer> snapshot = query.snapshot(neo4j_Bj.graphDb, tpInput, tint);
        snapshot.forEach((entity, value) -> 
            System.out.println("Road " + entity + " congestion level: " + value));
        neo4j_Bj.shutDown();
    }
}
