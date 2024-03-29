package com.kosei.adcreatorworkflow.hadoop.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 *
 */
public class GoogleCatalogInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new GoogleCatalogRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return true;
    }
}