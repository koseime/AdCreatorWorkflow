package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 */
public class KoseiTextParserReducer extends
        Reducer<Text, AdCreatorAssetsWritable, NullWritable, AdCreatorAssetsWritable> {

    private final static Logger LOG = Logger.getLogger(KoseiTextParserReducer.class.getName());

    @Override
    public void reduce(Text key, Iterable<AdCreatorAssetsWritable> values, Context context)
            throws IOException, InterruptedException {
        AdCreatorAssetsWritable mostRecent = null;
        long mostRecentTimestamp = -1;
        for (AdCreatorAssetsWritable value : values) {
            long timestamp = ByteBuffer.wrap(value.getMeta(new Text("timestamp")).copyBytes()).getLong();
            if (mostRecentTimestamp == -1 || timestamp > mostRecentTimestamp) {
                mostRecentTimestamp = timestamp;
                mostRecent = value;
            }
        }

        context.write(NullWritable.get(), mostRecent);
    }
}
