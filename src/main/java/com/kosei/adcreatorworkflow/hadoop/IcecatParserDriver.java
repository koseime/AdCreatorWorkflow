package com.kosei.adcreatorworkflow.hadoop;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 *
 * @author root
 */
public class IcecatParserDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);

        } catch (IOException ex) {
            Logger.getLogger(IcecatParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        conf.set("xmlinput.start", "<ICECAT-interface ");
        conf.set("xmlinput.end", "</ICECAT-interface>");
        conf
                .set(
                        "io.serializations",
                        "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        Job job = new Job(conf, "IcecatParser:" + input);


        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(IcecatParserDriver.class);
        job.setMapperClass(IcecatXMLParserMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(XmlInputFormat.class);
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
            Logger.getLogger(IcecatParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(IcecatParserDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}