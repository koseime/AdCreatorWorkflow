package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class KoseiTextParserMapper extends
        Mapper<Text, Text, Text, AdCreatorAssetsWritable> {

    static enum ParserEnum { INPUTREC, SUCCESS , OUTOFSTOCK}

    private String jobTimestamp = "";

    @Override
    public void setup(Context context) {
        jobTimestamp = context.getConfiguration().get("job.timestamp");
    }

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        String koseiCatRecord = value.toString();
        context.getCounter(ParserEnum.INPUTREC).increment(1);

        try {
            KoseiProductItem productItem = KoseiProductItem.parse(koseiCatRecord);

            String productId = productItem.getId();
            String[] imageURIs = productItem.getAllImageURIs();
            String productDesc = productItem.getTitle();
            String longProductDesc = productItem.getDescription();
            String category = productItem.getCleanCategory();
            String availability = productItem.getAvailability();
            long timestamp = Long.parseLong(productItem.getTimestamp());
            String catalogId = productItem.getCatalogId();
            String advertiserId = productItem.getAdvertiserId();

            if (productId.isEmpty()) { return; }
            AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(productId,
                    imageURIs, AdCreatorAssetsWritable.STATUS_RAW, null, productDesc, longProductDesc);

            byte[] timestampBytes = ByteBuffer.allocate(8).putLong(timestamp).array();
            ad.putMeta(new Text("job_timestamp"), new BytesWritable(jobTimestamp.getBytes()));
            ad.putMeta(new Text("timestamp"), new BytesWritable(timestampBytes));
            ad.putMeta(new Text("catalog_id"), new BytesWritable(catalogId.getBytes()));
            ad.putMeta(new Text("advertiser_id"), new BytesWritable(advertiserId.getBytes()));
            if (availability.equals("out of stock")) {
                context.getCounter(ParserEnum.OUTOFSTOCK).increment(1);
                ad.putMeta(new Text("category"), new BytesWritable("DELETED".getBytes()));
                ad.putMeta(new Text("deleted"), new BytesWritable(availability.getBytes()));
            } else {
                ad.putMeta(new Text("category"), new BytesWritable(category.getBytes()));
            }

            String id = catalogId + "_" + advertiserId + "_" + productId;
            context.write(new Text(id), ad);
            context.getCounter(ParserEnum.SUCCESS).increment(1);
        } catch (Exception e) {
            Logger.getLogger(KoseiTextParserMapper.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
