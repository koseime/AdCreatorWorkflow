package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by chantat on 8/15/14.
 */
public class GoogleTextParserReducer extends
        Reducer<Text, AdCreatorAssetsWritable, NullWritable, AdCreatorAssetsWritable> {

    private final static Logger LOG = Logger.getLogger(GoogleTextParserReducer.class.getName());

    @Override
    public void reduce(Text key, Iterable<AdCreatorAssetsWritable> values, Context context)
            throws IOException, InterruptedException {
        if (key.toString().equals("")) {
            String outputMeta = context.getConfiguration().get("meta.output");
            Path file = new Path(outputMeta);
            FileSystem hdfs = FileSystem.get(file.toUri(), context.getConfiguration());

            if (hdfs.exists(file)) { hdfs.delete(file, true); }

            FSDataOutputStream out = hdfs.create(file);
            for (AdCreatorAssetsWritable value : values) {
                byte[] categoryBytes = value.getMeta(new Text("category")).copyBytes();
                String line = value.getId() + "=" + new String(categoryBytes) + "\n";
                out.write(line.getBytes());
            }

            out.close();
        } else {
            boolean isFirst = true;
            for (AdCreatorAssetsWritable value : values) {
                context.write(NullWritable.get(), value);

                if(!isFirst) {
                    LOG.log(Level.INFO, "Multiple entries with identical product id: " + value.getId());
                }
                isFirst = false;
            }
        }
    }
}
