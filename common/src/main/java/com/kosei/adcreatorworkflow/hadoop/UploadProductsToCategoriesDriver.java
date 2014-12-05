package com.kosei.adcreatorworkflow.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class UploadProductsToCategoriesDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);
        } catch (IOException ex) {
            Logger.getLogger(UploadProductsToCategoriesDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "UploadProductsToCategoriesDriver:" + input);

        job.getConfiguration().set("base.url", "http://a");
        job.getConfiguration().set("api.token", "1");

        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(UploadProductsToCategoriesDriver.class);
        job.setMapperClass(UploadProductsToCategoriesMapper.class);
        job.setReducerClass(UploadProductsToCategoriesReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }


        try {
            job.waitForCompletion(true);
        } catch (InterruptedException ex) {
            Logger.getLogger(UploadProductsToCategoriesDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(UploadProductsToCategoriesDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}