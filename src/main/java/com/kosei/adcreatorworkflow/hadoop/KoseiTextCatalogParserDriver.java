package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.adcreatorworkflow.hadoop.io.GoogleCatalogInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class KoseiTextCatalogParserDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);
        } catch (IOException ex) {
            Logger.getLogger(KoseiTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "KoseiTextCatalogParserDriver:" + input);

        job.getConfiguration().set("job.timestamp", "1413413031");

        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(KoseiTextCatalogParserDriver.class);
        job.setMapperClass(KoseiTextParserMapper.class);
        job.setReducerClass(KoseiTextParserReducer.class);
        job.setNumReduceTasks(5);
        job.setInputFormatClass(GoogleCatalogInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AdCreatorAssetsWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(AdCreatorAssetsWritable.class);
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }


        try {
            job.waitForCompletion(true);
        } catch (InterruptedException ex) {
            Logger.getLogger(KoseiTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(KoseiTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}