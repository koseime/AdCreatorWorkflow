package com.kosei.adcreatorworkflow.hadoop;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 *
 *
 */
public class InputCollector {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String input = args[0];
        String output = args[1];
        String type = args[2];

        Configuration conf = new Configuration();
        Path outPath = new Path(output);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }

        Job job = new Job(conf, "InputCollector:" + input);


        if (type.equals("NEW_CATALOG")) {
            FileInputFormat.setInputPaths(job, input);
        } else if (type.equals("NEW_CAMPAIGN")) {
            String inputPaths = constructInputPaths(input, dfs);
            if (inputPaths.isEmpty()) { return; }
            FileInputFormat.setInputPaths(job, inputPaths);
            FileInputFormat.setInputDirRecursive(job, true);
        } else {
            throw new RuntimeException("Workflow type not specified");
        }

        job.setJarByClass(InputCollector.class);
        job.setMapperClass(CatalogCompactorMapper.class);
        job.setReducerClass(CatalogCompactorReducer.class);
        job.setNumReduceTasks(20);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        FileOutputFormat.setOutputPath(job, outPath);

        if (!job.waitForCompletion(true)) { throw new RuntimeException("Job failed"); }
    }

    private static String constructInputPaths(String baseDirString, FileSystem dfs) throws IOException {
        Path baseDir = new Path(baseDirString);
        Path rollupDir = new Path(baseDirString + "/rollup");
        StringBuilder inputPath = new StringBuilder();

        long latestArchivedTimestamp = 0;
        try {
            FileStatus[] status = dfs.listStatus(rollupDir);
            for (int i = 0; i < status.length; i++) {
                String filename = status[i].getPath().getName();
                long timestamp = Long.parseLong(FilenameUtils.removeExtension(filename));
                if (timestamp > latestArchivedTimestamp) {
                    latestArchivedTimestamp = timestamp;
                }
            }
            inputPath.append(baseDirString + "/rollup");
        } catch (Exception e) { }

        FileStatus[] status = dfs.listStatus(baseDir);
        for (int i = 0; i < status.length; i++) {
            String fullPath = status[i].getPath().toString();
            String filename = status[i].getPath().getName();
            if (filename.endsWith("READY")) {
                String parts[] = filename.split("-");
                Long timestamp = Long.parseLong(parts[1]);
                if (timestamp > latestArchivedTimestamp) {
                    if (inputPath.length() != 0) { inputPath.append(","); }
                    inputPath.append(FilenameUtils.removeExtension(fullPath));
                    inputPath.append(".images*");
                }
            }
        }

        return inputPath.toString();
    }
}
