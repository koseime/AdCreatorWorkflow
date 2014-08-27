package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by chantat on 8/27/14.
 */
public class CatalogCompactorReducer extends
        Reducer<Text, BytesWritable, NullWritable, BytesWritable> {
    private final static Logger LOG = Logger.getLogger(CatalogCompactorReducer.class.getName());

    @Override
    public void reduce(Text key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {

        BytesWritable newestValue = null;
        long recentTimestamp = 0;
        for (BytesWritable value : values) {
            AdComponents adComponents = AdComponents.parseFrom(value.copyBytes());
            long timestamp = 0;
            boolean timestampFound = false;
            for (int i = 0; i < adComponents.getMetaCount(); i++) {
                AdComponents.Meta metaEntry = adComponents.getMeta(i);
                if (metaEntry.getKey().equals("timestamp")) {
                    timestampFound = true;
                    timestamp = ByteBuffer.wrap(metaEntry.getValue().toByteArray()).getLong();
                    break;
                }
            }

            if (timestampFound && (recentTimestamp == 0 || timestamp > recentTimestamp)) {
                newestValue = value;
                recentTimestamp = timestamp;
            }
        }

        if (newestValue != null) {
            context.write(NullWritable.get(), newestValue);
        } else {
            LOG.log(Level.SEVERE, "Timestamp not found for productId: " + key.toString());
        }
    }

}
