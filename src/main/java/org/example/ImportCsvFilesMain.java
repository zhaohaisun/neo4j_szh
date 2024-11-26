package org.example;

import java.io.File;
import java.io.IOException;

import static org.example.CSVImporter.*;

public class ImportCsvFilesMain {
    public static void main(final String[] args) throws IOException {
        File csvFile = new File("src\\main\\java\\org\\example\\Topo.csv");
        File DyRoadInfoFile = new File("src\\main\\java\\org\\example\\100501.csv");
        //neo4j neo4j_Bj = new neo4j();
        neo4j_Bj.createDb();
        importCSV(csvFile, neo4j_Bj.graphDb);
        importDyRoadInfo(DyRoadInfoFile, neo4j_Bj.graphDb);
        neo4j_Bj.shutDown();
    }
}
