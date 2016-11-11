package com.dataimport;

import org.json.JSONObject;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hexu on 2016/9/21.
 */
public class EdgeImport{

    BatchInserter inserter;
    BatchInserterIndexProvider indexProvider;
    PrintWriter printWriter;

    private final String AUTHOR_INDEX = "author";
    private final String PAPER_INDEX = "paper";
    private final String INSTITUTION_INDEX = "institution";
    private final String KEYWORD_INDEX = "keyword";
    private final String WORK_IN_INDEX = "work";

    Label labelPaper, labelAuthor, labelInstitution, labelKeyord, labelPublish,
            labelWork_in, labelInvolve;

    BatchInserterIndex author_index, paper_index, institution_index, keyword_index,
    publish_index, work_index;

    public EdgeImport(String pathNeo4jDatabase) throws IOException{
        labelPaper  = DynamicLabel.label("Paper");
        labelAuthor = DynamicLabel.label("Author");
        labelInstitution = DynamicLabel.label("Institution");
        labelKeyord = DynamicLabel.label("Keyword");
        labelPublish = DynamicLabel.label("Publish");
        labelWork_in = DynamicLabel.label("Work_In");
        labelInvolve = DynamicLabel.label("Involve");

        InitializeInserter(pathNeo4jDatabase);
    }

    public void InitializeInserter(String pathNeo4jDatabse) throws IOException{
        inserter = BatchInserters.inserter(new File(pathNeo4jDatabse));
        indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        author_index = indexProvider.nodeIndex(AUTHOR_INDEX, MapUtil.stringMap("type", "exact"));
//        user_index.setCacheCapacity("name", );
        paper_index = indexProvider.nodeIndex(PAPER_INDEX, MapUtil.stringMap("type", "exact"));

        institution_index = indexProvider.nodeIndex(INSTITUTION_INDEX, MapUtil.stringMap("type", "exact"));

        keyword_index = indexProvider.nodeIndex(KEYWORD_INDEX, MapUtil.stringMap("type", "exact"));

        work_index = indexProvider.relationshipIndex(WORK_IN_INDEX, MapUtil.stringMap("type", "exact"));
    }

    public void importEdge(String itemDocStr, int paperNodeCount){
        JSONObject itemDocJson = new JSONObject(itemDocStr);
        /**
         * 开始导入节点
         */
        ConvertToNode convert = new ConvertToNode();
        List<String> institutuons = convert.getInstitution(itemDocJson);
        List<String> authors = convert.getAuthor(itemDocJson);
        if(institutuons.size()>1 && institutuons.size()!=authors.size()){
            return;
        }
        //导入论文节点
        long paperId;
        Map<String, Object> paper = convert.getPaper(itemDocJson);
        Map<String, Object> paperIndex = new HashMap<String, Object>();
        paperIndex.put("title", paper.get("title"));
        System.out.println(paper.get("title"));
        IndexHits<Long> getPaperIndex = paper_index.get("title", paper.get("title"));
        if(getPaperIndex.hasNext()){
            return;
        }else{
//            建立节点
            paperId = inserter.createNode(paper, labelPaper);
//            添加索引
            paper_index.add(paperId, paperIndex);
            paper_index.flush();
        }

        //导入机构节点

        for(String ins:institutuons){
            Map<String, Object> instition = new HashMap<String, Object>();
            instition.put("name", ins);
            IndexHits<Long> getInstitutionIndex = institution_index.get("name", ins);
            if(getInstitutionIndex.hasNext()){

            }else{
                long institutionId = inserter.createNode(instition, labelInstitution);
                institution_index.add(institutionId, instition);
                institution_index.flush();
            }
        }

        //导入作者结点,然后倒入作者与机构之间的关系，还有作者与论文之间的关系

        if(authors.size() == 1){
            Map<String, Object> author = new HashMap<String, Object>();
            author.put("name", authors.get(0));
            IndexHits<Long> getAuthorIndex = author_index.get("name", authors.get(0));
            IndexHits<Long> getInstitutionIndex = institution_index.get("name", institutuons.get(0));
            long institutionIndexId = getInstitutionIndex.getSingle();
            if(getAuthorIndex.hasNext()) {
                boolean flag = false;
                while (getAuthorIndex.hasNext()) {
                    long authorIndexId = getAuthorIndex.next();
                    String work_index_name = Long.toString(authorIndexId) + "-" + Long.toString(institutionIndexId);
                    IndexHits<Long> getWorkIndex = work_index.get("name", work_index_name);
                    if (getWorkIndex.hasNext()) {
                        //产生作者与论文之间的关系，不需要索引
                        inserter.createRelationship(authorIndexId, paperId, DynamicRelationshipType.withName("publish"), null);
                        flag = true;
                        break;
                    }
                }
                if(flag == false){
                    //产生新的作者节点
                    long authorId = inserter.createNode(author, labelAuthor);
                    author_index.add(authorId, author);
                    author_index.flush();

                    //产生作者与机构之间的关系，唯一索引
                    String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                    Map<String, Object> index_value = new HashMap<String, Object>();
                    index_value.put("name", new_work_index_name);
                    long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                    work_index.add(workId, index_value);
                    work_index.flush();

                    //产生作者与论文之间的关系，不需要索引
                    inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
                }
            }else{
                //生成新的作者结点
                long authorId = inserter.createNode(author, labelAuthor);
                author_index.add(authorId, author);
                author_index.flush();

                //创建作者与机构之间的关系，唯一索引
                String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                Map<String, Object> index_value = new HashMap<String, Object>();
                index_value.put("name", new_work_index_name);
                long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                work_index.add(workId, index_value);
                work_index.flush();

                //产生作者与论文之间的关系，不需要索引
                inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
            }

        }else{
            for(int i=0; i<authors.size(); i++){
                Map<String, Object> author = new HashMap<String, Object>();
                author.put("name", authors.get(i));
//                System.out.println(authors.get(i));
                IndexHits<Long> getAuthorIndex = author_index.get("name", authors.get(i));
                if(getAuthorIndex.hasNext()){
                    if(institutuons.size() == 1){
                        IndexHits<Long> getInstitutionIndex = institution_index.get("name", institutuons.get(0));
                        long institutionIndexId = getInstitutionIndex.getSingle();
                        boolean flag = false;
                        while(getAuthorIndex.hasNext()){
                            long authorIndexId = getAuthorIndex.next();
                            String work_index_name = Long.toString(authorIndexId) + "-" + Long.toString(institutionIndexId);
                            IndexHits<Long> getWorkIndex = work_index.get("name", work_index_name);
                            if (getWorkIndex.hasNext()) {
                                //产生作者与论文之间的关系，不需要索引
                                inserter.createRelationship(authorIndexId, paperId, DynamicRelationshipType.withName("publish"), null);
                                flag = true;
                                break;
                            }
                        }
                        if(flag == false){
                            //产生新的作者节点
                            long authorId = inserter.createNode(author, labelAuthor);
                            author_index.add(authorId, author);
                            author_index.flush();

                            //产生作者与机构之间的关系，唯一索引
                            String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                            Map<String, Object> index_value = new HashMap<String, Object>();
                            index_value.put("name", new_work_index_name);
                            long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                            work_index.add(workId, index_value);
                            work_index.flush();

                            //产生作者与论文之间的关系，不需要索引
                            inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
                            break;
                        }
                    }else{
                        IndexHits<Long> getInstitutionIndex = institution_index.get("name", institutuons.get(i));
                        long institutionIndexId = getInstitutionIndex.getSingle();
                        boolean flag = false;
                        while(getAuthorIndex.hasNext()){                                                      //问题在循环
                            long authorIndexId = getAuthorIndex.next();
                            String work_index_name = Long.toString(authorIndexId) + "-" + Long.toString(institutionIndexId);
                            IndexHits<Long> getWorkIndex = work_index.get("name", work_index_name);
                            if (getWorkIndex.hasNext()) {
                                //产生作者与论文之间的关系，不需要索引
                                inserter.createRelationship(authorIndexId, paperId, DynamicRelationshipType.withName("publish"), null);
                                flag = true;
                                break;
                            }
                            }
                            if(flag == false){
                                //产生新的作者节点
                                long authorId = inserter.createNode(author, labelAuthor);
                                author_index.add(authorId, author);
                                author_index.flush();

                                //产生作者与机构之间的关系，唯一索引
                                String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                                Map<String, Object> index_value = new HashMap<String, Object>();
                                index_value.put("name", new_work_index_name);
                                long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                                work_index.add(workId, index_value);
                                work_index.flush();

                                //产生作者与论文之间的关系，不需要索引
                                inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
                            }
                        }
                }else{
                    if(institutuons.size() == 1){
                        //建立新的作者结点
                        long authorId = inserter.createNode(author, labelAuthor);
                        author_index.add(authorId, author);
                        author_index.flush();

                        //创建作者与机构之间的关系，唯一索引
                        IndexHits<Long> getInstitutionIndex = institution_index.get("name", institutuons.get(0));
                        long institutionIndexId = getInstitutionIndex.getSingle();
                        String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                        Map<String, Object> index_value = new HashMap<String, Object>();
                        index_value.put("name", new_work_index_name);

                        long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                        work_index.add(workId, index_value);
                        work_index.flush();

                        //产生作者与论文之间的关系，不需要索引
                        inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
                    }else{
                        //建立新的作者结点
                        long authorId = inserter.createNode(author, labelAuthor);
                        author_index.add(authorId, author);
                        author_index.flush();

                        //创建作者与机构之间的关系，唯一索引
                        IndexHits<Long> getInstitutionIndex = institution_index.get("name", institutuons.get(i));
                        long institutionIndexId = getInstitutionIndex.getSingle();
                        String new_work_index_name = Long.toString(authorId) + "-" + Long.toString(institutionIndexId);
                        Map<String, Object> index_value = new HashMap<String, Object>();
                        index_value.put("name", new_work_index_name);

                        long workId = inserter.createRelationship(authorId, institutionIndexId, DynamicRelationshipType.withName("work_in"), index_value);
                        work_index.add(workId, index_value);
                        work_index.flush();

                        //产生作者与论文之间的关系，不需要索引
                        inserter.createRelationship(authorId, paperId, DynamicRelationshipType.withName("publish"), null);
                    }

                }

            }
        }


        //导入关键词节点
        //会出现同一个节点指向一个关键词几次的情况，应该是这个关键词在该文里面重复了，查看了一下，确定
        List<String> keywords = convert.getKeyWords(itemDocJson);
        for(String key:keywords){
            Map<String, Object> keyword = new HashMap<String, Object>();
            keyword.put("name", key);
            IndexHits<Long> getKeywordIndex = keyword_index.get("name", key);
            if(getKeywordIndex.hasNext()){
                inserter.createRelationship(paperId, getKeywordIndex.getSingle(), DynamicRelationshipType.withName("involve"), null);
            }else{
                long keywordId = inserter.createNode(keyword, labelKeyord);
                keyword_index.add(keywordId, keyword);
                keyword_index.flush();
                inserter.createRelationship(paperId, keywordId, DynamicRelationshipType.withName("involve"), null);
            }
        }
        }

    public void shutDownIndex(){
        indexProvider.shutdown();
    }
    public void shutDownNeo4j(){
        inserter.shutdown();
    }
    public void close(){
        inserter.shutdown();
        printWriter.close();
    }
}