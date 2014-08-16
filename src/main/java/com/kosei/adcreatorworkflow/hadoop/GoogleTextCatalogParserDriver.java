package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class GoogleTextCatalogParserDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);
        } catch (IOException ex) {
            Logger.getLogger(GoogleTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        conf
                .set(
                        "io.serializations",
                        "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        Job job = new Job(conf, "GoogleTextCatalogParserDriver:" + input);


        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(GoogleTextCatalogParserDriver.class);
        job.setMapperClass(GoogleTextParserMapper.class);
        job.setReducerClass(GoogleTextParserReducer.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
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
            Logger.getLogger(GoogleTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(GoogleTextCatalogParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}