package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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
public class PipesOutputParserDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);

        } catch (IOException ex) {
            Logger.getLogger(ImageCrawlerDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        conf
                .set(
                        "io.serializations",
                        "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        Job job = new Job(conf, "ImageCrawler:" + input);


        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(ImageCrawlerDriver.class);
        job.setMapperClass(PipesOutputParserMapper.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setReducerClass(PipesOutputParserReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }


        try {

            job.waitForCompletion(true);

        } catch (InterruptedException ex) {
            Logger.getLogger(ImageCrawlerDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ImageCrawlerDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}