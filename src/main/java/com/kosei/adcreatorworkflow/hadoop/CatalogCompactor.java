package com.kosei.adcreatorworkflow.hadoop;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
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
public class CatalogCompactor {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        String advertisersRoot = args[0];

        Path advertisersPath = new Path(advertisersRoot);
        Configuration conf = new Configuration();
        FileSystem dfs = FileSystem.get(advertisersPath.toUri(), conf);

        FileStatus[] advertiserList = dfs.listStatus(advertisersPath);
        for (int i = 0; i < advertiserList.length; i++) {
            String filename = advertiserList[i].getPath().getName();
            if (StringUtils.isNumeric(filename)) {
                Path archivePath = new Path(advertiserList[i].getPath().toString() + "/catalog/archive");
                FileStatus[] archiveList = null;
                try {
                    archiveList = dfs.listStatus(archivePath);
                } catch (Exception e) {
                    archiveList = new FileStatus[0];
                }
                for (int j = 0; j < archiveList.length; j++) {
                    String catalogPath = archiveList[j].getPath().getName();
                    if (StringUtils.isNumeric(catalogPath)) {
                        System.out.println(archiveList[j].getPath().toString());
                        runJob(archiveList[j].getPath().toString());
                    }
                }
            }
        }
    }

    public static void runJob(String baseDir) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem dfs = FileSystem.get((new Path(baseDir)).toUri(), conf);

        Job job = new Job(conf, "InputCollector:" + baseDir);

        long[] archiveRange = getArchiveRange(baseDir, dfs);
        removePreviouslyArchived(baseDir, archiveRange[0], dfs);
        System.out.println("Archive range: " + archiveRange[0] + " - " + archiveRange[1]);
        if (archiveRange[1] > 0) {
            FileInputFormat.setInputPaths(job, constructInputPaths(baseDir, archiveRange, dfs));
            FileInputFormat.setInputDirRecursive(job, true);

            dfs.mkdirs(new Path(baseDir + "/rollup"));

            Path outPath = new Path(baseDir + "/rollup/" + Long.toString(archiveRange[1]));

            job.setJarByClass(CatalogCompactor.class);
            job.setMapperClass(CatalogCompactorMapper.class);
            job.setReducerClass(CatalogCompactorReducer.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(BytesWritable.class);

            FileOutputFormat.setOutputPath(job, outPath);

            job.waitForCompletion(true);
        }
    }

    private static long[] getArchiveRange(String baseDirString, FileSystem dfs)
            throws IOException {
        Path rollupDir = new Path(baseDirString + "/rollup");
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
        } catch (Exception e) { }

        Path baseDir = new Path(baseDirString);
        long latestTimestamp = 0;
        FileStatus[] status = dfs.listStatus(baseDir);
        for (int i = 0; i < status.length; i++) {
            String filename = status[i].getPath().getName();
            if (filename.endsWith("READY")) {
                String parts[] = filename.split("-");
                Long timestamp = Long.parseLong(parts[1]);
                if (timestamp > latestArchivedTimestamp && timestamp > latestTimestamp) {
                    latestTimestamp = timestamp;
                }
            }
        }

        return new long[] {latestArchivedTimestamp, latestTimestamp};
    }

    private static void removePreviouslyArchived(String baseDirString, long latestArchivedTimestamp, FileSystem dfs)
            throws IOException {
        Path baseDir = new Path(baseDirString);
        FileStatus[] status = dfs.listStatus(baseDir);
        for (int i = 0; i < status.length; i++) {
            String filename = status[i].getPath().getName();
            if (filename.equals("rollup")) {
                continue;
            }
            String parts[] = filename.split("-");
            Long timestamp = Long.parseLong(parts[1]);
            if (timestamp <= latestArchivedTimestamp) {
                dfs.delete(status[i].getPath(), true);
            }
        }
    }


    private static String constructInputPaths(String baseDirString, long[] archiveRange, FileSystem dfs)
            throws IOException {
        Path baseDir = new Path(baseDirString);
        StringBuilder inputPath = new StringBuilder();

        FileStatus[] status = dfs.listStatus(baseDir);
        for (int i = 0; i < status.length; i++) {
            String fullPath = status[i].getPath().toString();
            String filename = status[i].getPath().getName();
            if (filename.endsWith("READY")) {
                String parts[] = filename.split("-");
                Long timestamp = Long.parseLong(parts[1]);
                if (timestamp > archiveRange[0] && timestamp <= archiveRange[1]) {
                    if (inputPath.length() != 0) { inputPath.append(","); }
                    System.out.println(FilenameUtils.removeExtension(fullPath) + ".images*");
                    inputPath.append(FilenameUtils.removeExtension(fullPath));
                    inputPath.append(".images*");
                }
            }
        }

        return inputPath.toString();
    }
}
