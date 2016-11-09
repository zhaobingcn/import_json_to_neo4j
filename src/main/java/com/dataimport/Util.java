package com.dataimport;

import java.io.*;

/**
 * Created by hexu on 2016/9/22.
 */
public class Util {
    //读取json文件，变成流文件
    public static BufferedReader getBufferedReaderForJson(String file) throws FileNotFoundException {
        File inFilePath = new File(file);
        FileInputStream fileInputStream = new FileInputStream(inFilePath);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader read = new BufferedReader(inputStreamReader);
        return read;
    }
}
