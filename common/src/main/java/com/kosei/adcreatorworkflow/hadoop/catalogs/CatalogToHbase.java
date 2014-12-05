package com.kosei.adcreatorworkflow.hadoop.catalogs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




/**
 * Created by hess on 11/17/14.
 */
public class CatalogToHbase {

  public static void main(String[] args) throws Exception {
    String baseDir = args[0];
    String quorumAddress = args[1];
    String tableName = args[2];

    Configuration conf = new Configuration();
    Path inPath = new Path(baseDir + "/catalogItems");

    String[] otherArgs =
        new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "Import stuff to HBase");

    job.setJarByClass(CatalogToHbase.class);
    job.setMapperClass(CatalogToHbaseMapper.class);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);

    // Here is the QUORUM_ADDRESS value for Kosei Prod:
    // the format is <comma-separated list of HBase ZK servers>:<ZK Port>:<ZK hbase node>
    // the value for CDH prod cluster is:
    // "hbasemaster.internal.kosei.me,hbase1.internal.kosei.me,hbase3.internal.kosei.me,hbase2.internal.kosei.me:2181:/hbase"
    job.getConfiguration().set(TableOutputFormat.QUORUM_ADDRESS,quorumAddress);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, inPath);

    job.setNumReduceTasks(0);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
