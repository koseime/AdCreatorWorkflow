package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class GoogleTextParserMapper extends
        Mapper<LongWritable, Text, Text, AdCreatorAssetsWritable> {

    static enum ParserEnum { INPUTREC, SUCCESS }

    @Override
    public void map(LongWritable key, Text value1,Context context)
            throws IOException, InterruptedException {

        String timestampString = context.getConfiguration().get("timestamp");
        long timestamp = Long.parseLong(timestampString);
        byte[] timestampBytes = ByteBuffer.allocate(8).putLong(timestamp).array();

        String googleCatRecord = value1.toString();
        context.getCounter(ParserEnum.INPUTREC).increment(1);

        try {
            GoogleProductItem gpi = GoogleProductItem.fromParse(googleCatRecord);

            String productId =gpi.getId();
            String lowPicURI =gpi.getAdditionalImageLink();
            String thumbPicURI =gpi.getAdditionalImageLink();
            String productDesc = gpi.getTitle();
            String longProductDesc = gpi.getDescription();
            String category = cleanCategory(gpi.getGoogleProductCategory());

            if (productId.isEmpty()) { return; }
            AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(productId, lowPicURI, thumbPicURI,
                    AdCreatorAssetsWritable.STATUS_RAW, null, productDesc, longProductDesc);
            ad.putMeta(new Text("category"), new BytesWritable(category.getBytes()));
            ad.putMeta(new Text("timestamp"), new BytesWritable(timestampBytes));

            context.write(new Text(""), ad);
            context.write(new Text(ad.getId()), ad);
            context.getCounter(ParserEnum.SUCCESS).increment(1);

        } catch (IOException ex) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private String cleanCategory(String category) {
        category = category.replaceAll("(\\s*)&gt;(\\s*)", ">");
        category = category.replaceAll("&amp;", "&");
        return category;
    }

}
