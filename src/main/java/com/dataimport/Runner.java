package com.dataimport;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.store.IOContext;
import org.json.JSONObject;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.register.Register;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hexu on 2016/9/21.
 */
public class Runner {

    public static void main(String[] args) throws IOException{
        BufferedReader inputReader = null;
        try{
           inputReader = Util.getBufferedReaderForJson("D:/paper.json");
        }catch(IOException e){
            e.printStackTrace();
        }
        String dbPath = "D:/ProfessionalSoftware/Neo4jDB/Importdata";

        EdgeImport edgeImport = new EdgeImport(dbPath);
        int paperNodeCount = 0;
        while(true){

            String tempDocStr = inputReader.readLine();
            if(tempDocStr == null || tempDocStr.trim().equals(""))
                break;
            edgeImport.importEdge(tempDocStr, paperNodeCount);
            paperNodeCount++;
        }

         edgeImport.shutDownIndex();
         edgeImport.shutDownNeo4j();
    }
}
