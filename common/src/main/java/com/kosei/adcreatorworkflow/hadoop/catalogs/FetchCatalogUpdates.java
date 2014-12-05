package com.kosei.adcreatorworkflow.hadoop.catalogs;

import com.kosei.adcreatorworkflow.hadoop.CatalogCompactorMapper;
import com.kosei.adcreatorworkflow.hadoop.CatalogCompactorReducer;
import com.kosei.adcreatorworkflow.hadoop.catalogs.data.NullInputFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by hess on 11/17/14.
 */
public class FetchCatalogUpdates {

  public static void main(String args[])
      throws IOException, ClassNotFoundException, InterruptedException {
    String baseDir = args[0];

    Configuration conf = new Configuration();
    FileSystem dfs = FileSystem.get((new Path(baseDir)).toUri(), conf);

    Job job = new Job(conf, "Fetch Catalog Updates:" + baseDir);
    Path outPath = new Path(baseDir + "/fetchCatalogUpdates");
    dfs.delete(outPath, true);
    job.getConfiguration().set("managementClientApiToken","igh8caahbenv2rc09edboa5vsn0a1pdm2sru6pitsatk7c4bh3");
    job.getConfiguration().set("managementClientApiEndPoint", "https://sentry.kosei.com");
    job.getConfiguration().set("awsAccessKey","AKIAIVEJZIYPGUU4T3WQ");
    job.getConfiguration().set("awsSecretKey","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");
    job.setJarByClass(FetchCatalogUpdates.class);
    job.setMapperClass(FetchCatalogUpdatesMapper.class);
    job.setReducerClass(FetchCatalogUpdatesReducer.class);
    job.setInputFormatClass(NullInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    FileOutputFormat.setOutputPath(job, outPath);

    if (!job.waitForCompletion(true)) {
      throw new RuntimeException("Job failed: " + baseDir);
    }
  }
}
