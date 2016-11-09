package com.dataimport;

/**
 * Created by hexu on 2016/9/22.
 */
public class Config {
    public static int restartNodeNum = 2000000; // after "restartNodeNum" items, we restart neo4j (when importing nodes)
    public static int printNodeNum = 5000;      // after "printNodeNum" items, we print progress (when importing nodes)
}
