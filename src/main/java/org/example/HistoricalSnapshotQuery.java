package org.example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;

import static org.example.CSVImporter.neo4j_Bj;

import java.util.HashMap;
import java.util.Map;

public class HistoricalSnapshotQuery {
    /* 历史快照查询
     * snapshot(TG, tp, t) -> <(entity_1, value_1), ..., (entity_n, value_n)>
    public Map<String, Object> snapshot(String tp, int time) {
     * tp: 查询的时态属性
     * t: 时间点
     * 返回: (entity, value)组成的集合，entity表示点/边，value为该点/边在时间t的tp属性值
     */

    public Map<String, Object> snapshot(GraphDatabaseService graphDb, String tp, String time) {
        Map<String, Object> result = new HashMap<>();

        try (Transaction tx = graphDb.beginTx()) {
            // 获取所有关系并过滤
            for (Relationship relationship : tx.getAllRelationships()) {
                if (relationship.getType().name().equals("ROAD_TO")) {
                    // 检查时间属性
                    String relTime = (String) relationship.getProperty("time", "");
                    if (relTime.equals(time)) {
                        // 获取起始和终止节点
                        Node startNode = relationship.getStartNode();
                        Node endNode = relationship.getEndNode();
                        
                        // 构建实体标识符
                        String entity = startNode.getProperty("id") + "->" + endNode.getProperty("id");
                        
                        // 获取指定属性值
                        if (relationship.hasProperty(tp)) {
                            Object value = relationship.getProperty(tp);
                            result.put(entity, value);
                        }
                    }
                }
            }
            tx.commit();
        }

        return result;
    }

    // 使用示例
    public static void main(String[] args) {
        HistoricalSnapshotQuery query = new HistoricalSnapshotQuery();
        Map<String, Object> snapshot = query.snapshot(neo4j_Bj.graphDb, "congestionLevel", "05010005");
        snapshot.forEach((entity, value) -> 
            System.out.println("Road " + entity + " congestion level: " + value));
    }
}
