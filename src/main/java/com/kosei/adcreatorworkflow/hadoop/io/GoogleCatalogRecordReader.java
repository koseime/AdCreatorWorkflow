package com.kosei.adcreatorworkflow.hadoop.io;

import com.kosei.adcreatorworkflow.hadoop.GoogleProductItem;
import com.kosei.adcreatorworkflow.hadoop.GoogleProductItemParser;
import com.kosei.adcreatorworkflow.hadoop.KoseiProductItem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 *
 *
 */
public class GoogleCatalogRecordReader extends RecordReader<Text, Text> {
    private static final Log LOG = LogFactory.getLog(GoogleCatalogRecordReader.class);

    private GoogleProductItemParser googleProductItemParser;

    private String catalogId = "";
    private String advertiserId = "";
    private String timestamp = "";

    private LineReader in;
    private Text key;
    private Text value = new Text();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private int maxLineLength;

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public Text getCurrentKey() throws IOException,InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    private void readHeader(FileSystem fs, Configuration conf, Path path) throws IOException {
        String filename = path.getName();
        String parts[] = filename.split("[-\\.]");
        advertiserId = parts[0];
        catalogId = parts[2];
        timestamp = parts[1];

        FSDataInputStream filein = fs.open(path);
        LineReader reader = new LineReader(filein, conf);

        Text header = new Text();
        int len = reader.readLine(header, maxLineLength,
                Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        filein.close();
        if (len >= maxLineLength) { throw new IOException("Header is too long"); }
        googleProductItemParser = new GoogleProductItemParser(header.toString());
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();

        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        start = split.getStart();
        end = start + split.getLength();

        readHeader(fs, conf, split.getPath());

        FSDataInputStream filein = fs.open(split.getPath());
        if (start != 0) {
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein, conf);
        start += in.readLine(new Text(),maxLineLength,(int)Math.min((long)Integer.MAX_VALUE, end - start));

        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new Text();
        }
        key.set("a");

        Text rawValue = new Text();
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(rawValue, maxLineLength,
                    Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
            pos += newSize;
            if (newSize < maxLineLength) { break; }
            LOG.info("Skipped line of size " + newSize + " at pos " +
                    (pos - newSize));
        }

        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            GoogleProductItem googleProductItem = googleProductItemParser.parse(rawValue.toString());
            if (value == null) {
                value = new Text();
            }
            value.clear();
            value.set(KoseiProductItem.parse(googleProductItem, timestamp, catalogId, advertiserId).toString());
            return true;
        }
    }


}
