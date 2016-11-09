package com.dataimport;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by hexu  on 2016/9/22.
 */
public class ConvertToNode {

    public List<String> getAuthor(JSONObject object) {
        List<String> authors = new ArrayList<String>();
        if (object.getJSONArray("authors")==null) {
            return authors;
        } else {
            JSONArray auhtorArray = object.getJSONArray("authors");
            for (Object au : auhtorArray) {
                String author = au.toString();
                authors.add(author);
            }
            return authors;
        }
    }

    public List<String> getInstitution(JSONObject object) {
        List<String> institutions = new ArrayList<String>();
        if (object.get("institutions").toString().startsWith("[\"")) {
            if (object.getJSONArray("institutions") == null) {
                return institutions;
            } else {
                JSONArray institionArray = object.getJSONArray("institutions");
                for (Object in : institionArray) {
                    String instition = in.toString();
                    institutions.add(instition);
                }
                return institutions;
            }
        }else{
            institutions.add(object.getString("institutions"));
//            System.out.println(institutions.get(0));
//            List<String> authors = getAuthor(object);
//            for(String author:authors){
//                System.out.print(author + "---");
//            }
//            System.out.println("\n\n");
            return institutions;
        }
    }

    public List<String> getKeyWords(JSONObject object){
        List<String> keywords = new ArrayList<String>();
        if(object.getJSONArray("keywords")==null){
            return keywords;
        }else{
            JSONArray keywordsArray = object.getJSONArray("keywords");
            for (Object ke : keywordsArray) {
                String keyword = ke.toString();
                keywords.add(keyword);
            }
            return keywords;
        }
        }

    public Map<String, Object> getPaper(JSONObject object){
        Map<String, Object> paper = new HashMap<String, Object>();
        String title = "";
        int quote = 0;
        String link = "";
        String date = "";
        JSONArray dates = new JSONArray();
        if(object.getString("title")!=null){
            title = object.getString("title");
        }
        if(object.getString("quote")!=null){
            quote = Integer.parseInt(object.getString("quote"));
        }
        if(object.getString("link")!=null){
            link = object.getString("link");
        }
        if(object.getJSONArray("date")!=null){
            dates = object.getJSONArray("date");
            if(dates.length()>0){
                date = (dates.get(0)).toString();
            }

        }
        paper.put("title", title);
        paper.put("quote", quote);
        paper.put("link", link);
        paper.put("date", date);
        return paper;
    }

}