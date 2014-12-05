package com.kosei.adcreatorworkflow.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;

/**
 * Created by chantat on 9/2/14.
 */
public class GoogleHeaderExtractor {

    public static void main(String[] args)  throws InterruptedException, IOException {
        String catalogPath = args[0];
        String headerPath = args[1];

        Path inputFile = new Path(catalogPath);
        FileSystem hdfs = FileSystem.get(inputFile.toUri(), new Configuration());

        final BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(inputFile), "UTF-8"));
        String header = br.readLine();
        br.close();

        Path outputFile = new Path(headerPath);
        BufferedOutputStream bufferedOut = new BufferedOutputStream(hdfs.create(outputFile));
        bufferedOut.write(header.getBytes());
        bufferedOut.close();
    }
}
