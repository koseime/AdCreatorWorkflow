package com.kosei.adcreatorworkflow.hadoop.catalogs;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.NullInputFormat;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.ProductItemWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by hess on 11/17/14.
 */
public class ParseCatalogs {
  public static void main(String args[])
      throws IOException, ClassNotFoundException, InterruptedException {
    String baseDir = args[0];

    Configuration conf = new Configuration();
    FileSystem dfs = FileSystem.get((new Path(baseDir)).toUri(), conf);

    Job job = new Job(conf, "Parse Catalogs:" + baseDir);
    Path inPath = new Path(baseDir + "/fetchCatalogUpdates");
    Path outPath = new Path(baseDir + "/catalogItems");
    dfs.delete(outPath, true);

    job.getConfiguration().set("awsAccessKey","AKIAIVEJZIYPGUU4T3WQ");
    job.getConfiguration().set("awsSecretKey","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");

    job.setJarByClass(ParseCatalogs.class);
    job.setMapperClass(ParseCatalogsMapper.class);
    job.setReducerClass(ParseCatalogsReducer.class);

    job.getConfiguration().setBoolean("mapred.mapper.new-api", true);
    job.getConfiguration().setBoolean("mapred.reducer.new-api", true);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.setInputPaths(job, inPath);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ProductItemWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(ProductItemWritable.class);

    FileOutputFormat.setOutputPath(job, outPath);

    if (!job.waitForCompletion(true)) {
      throw new RuntimeException("Job failed: " + baseDir);
    }
  }

}
