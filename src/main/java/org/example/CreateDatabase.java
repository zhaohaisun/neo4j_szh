package org.example;

import java.nio.file.Path;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

import java.io.File;

public class CreateDatabase {
    DatabaseManagementService managementService;
    GraphDatabaseService graphDb;

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    // 创建并初始化DatabaseManagementService和GraphDatabaseService实例
    public void createDatabase() {
        // 定义数据库存储目录
        File databaseDirectory = new File("neo4j-db");
        Path databasePath = databaseDirectory.toPath();

        // 使用DatabaseManagementServiceBuilder启动嵌入式Neo4j数据库
        managementService = new DatabaseManagementServiceBuilder(databasePath).build();

        // 获取GraphDatabaseService实例
        graphDb = managementService.database("neo4j");
    }

    // 打开数据库
    public void openDatabase() {
        if (managementService == null) {
            createDatabase();
            System.out.println("Neo4j嵌入式数据库启动成功！");
        } else {
            System.out.println("数据库已经打开。");
        }
    }

    // 关闭数据库
    public void closeDatabase() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
            System.out.println("Neo4j嵌入式数据库已成功关闭。");
        } else {
            System.out.println("数据库已关闭或尚未启动。");
        }
    }

    // 注册关闭钩子，确保Neo4j数据库在应用程序关闭时正确关闭
    public void registerShutdownHook() {
        if (managementService != null) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    managementService.shutdown();
                    System.out.println("关闭钩子：Neo4j数据库已成功关闭。");
                }
            });
        } else {
            System.out.println("关闭钩子未注册，因为数据库尚未启动。");
        }
    }
}
