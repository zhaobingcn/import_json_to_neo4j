package com.dataimport;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhzy on 2016/9/25.
 */
public class Test {

    public static void main(String[] args) throws IOException{
        String dbPath = "D:/ProfessionalSoftware/Neo4jDB/importTest";

        BatchInserter inserter = BatchInserters.inserter(new File(dbPath));

        String USER_INDEX = "user";
        String RELATIONSHIP_INDEX = "friend";
        String INSTITUTION_INDEX = "institution";

        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);

        BatchInserterIndex user_index = indexProvider.nodeIndex(USER_INDEX, MapUtil.stringMap("type", "exact"));
        BatchInserterIndex friend_index = indexProvider.relationshipIndex(RELATIONSHIP_INDEX, MapUtil.stringMap("type", "exact"));
        BatchInserterIndex institution_index = indexProvider.nodeIndex(INSTITUTION_INDEX, MapUtil.stringMap("type", "exact"));

//        user_index.setCacheCapacity( "name", 100000 );

        Map<String, Object> user1 = new HashMap<String, Object>();
        user1.put("name", "zhao");
        inserter.createNode(100l,user1, DynamicLabel.label("User"));
        Map<String, Object> index = new HashMap<String, Object>();
        index.put("name", "zhao");
        user_index.add(100l, user1);
        user_index.flush();

        Map<String, Object> user3 = new HashMap<String, Object>();
        user3.put("name", "zhao");
        long id3 = inserter.createNode(user3, DynamicLabel.label("User"));
        user_index.add(id3, user3);
        user_index.flush();


        Map<String, Object> institution1 = new HashMap<String, Object>();
        institution1.put("name", "bupt");
        long ins1 = inserter.createNode(institution1, DynamicLabel.label("Institution"));
        institution_index.add(ins1, institution1);
        institution_index.flush();

        IndexHits<Long> indexHits = user_index.get("name", "zhao");
        if(indexHits.hasNext()){
            if(5 > 6){

            }
            else{
                int asd = 5;
                IndexHits<Long> asdasd = institution_index.get("name", "bupt");
                long sadasd = asdasd.getSingle();
                while(indexHits.hasNext()){
                    System.out.println(indexHits.next() + "+++++++++++");
                }
            }
        }
// else{
//            Map<String, Object> user2 = new HashMap<String, Object>();
//            user2.put("name", "zhao");
//            long id2 = inserter.createNode(user2, DynamicLabel.label("User"));
//            user_index.add(id2, user2);
//        }



        RelationshipType relation = DynamicRelationshipType.withName("friend");
        Map<String, Object> properties = new HashMap<String, Object>();
        String content = Long.toString(100l) + "-" +Long.toString(id3);
        properties.put("name", content);
        long relation_id = inserter.createRelationship(100l, id3, relation, properties);
        friend_index.add(relation_id, properties);

//        IndexHits<long> relation_index = friend_index.query()



        indexProvider.shutdown();
        inserter.shutdown();


    }
}
