package org.example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static org.example.CSVImporter.neo4j_Bj;

import java.util.HashMap;
import java.util.Map;

public class HistoricalSnapshotQuery {

    private GraphDatabaseService graphDb;

    public HistoricalSnapshotQuery(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }
    /* 历史快照查询
     * snapshot(TG, tp, t) -> <(entity_1, value_1), ..., (entity_n, value_n)>
    public Map<String, Object> snapshot(String tp, int time) {
     * tp: 查询的时态属性
     * t: 时间点
     * 返回: (entity, value)组成的集合，entity表示点/边，value为该点/边在时间t的tp属性值
     */

    public Map<String, Object> snapshot(GraphDatabaseService graphDb, String tp, String time) {
        Map<String, Object> result = new HashMap<>();
        
        // 构建Cypher查询
        String query = 
            "MATCH (start)-[r:ROAD_TO]->(end) " +
            "WHERE r.time = $time " +
            "RETURN start.name AS startNode, end.name AS endNode, r." + tp + " AS value";

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("time", time);

        // 执行查询
        try (Transaction tx = graphDb.beginTx()) {
            Result queryResult = tx.execute(query, parameters);
            while (queryResult.hasNext()) {
                Map<String, Object> row = queryResult.next();
                String entity = row.get("startNode") + "->" + row.get("endNode");
                Object value = row.get("value");
                result.put(entity, value);
            }
            tx.commit();
        }

        return result;
    }

    // 使用示例
    public static void main(String[] args) {
        HistoricalSnapshotQuery query = new HistoricalSnapshotQuery(neo4j_Bj.graphDb);
        Map<String, Object> snapshot = query.snapshot(neo4j_Bj.graphDb, "congestionLevel", "05010005");
        snapshot.forEach((entity, value) -> 
            System.out.println("Road " + entity + " congestion level: " + value));
    }
}
