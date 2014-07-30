package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
public class GoogleTextParserMapper extends
        Mapper<LongWritable, Text, NullWritable, AdCreatorAssetsWritable> {

    static enum ParserEnum { INPUTREC, SUCCESS }

    @Override
    public void map(LongWritable key, Text value1,Context context)
            throws IOException, InterruptedException {

        String googleCatRecord = value1.toString();
        context.getCounter(ParserEnum.INPUTREC).increment(1);

        try {
            GoogleProductItem gpi = GoogleProductItem.fromParse(googleCatRecord);

            String productId =gpi.getId();
            String lowPicURI =gpi.getAdditionalImageLink();
            String thumbPicURI =gpi.getAdditionalImageLink();
            String productDesc = gpi.getTitle();
            String longProductDesc = gpi.getDescription();

            AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(productId, lowPicURI, thumbPicURI,
                    AdCreatorAssetsWritable.STATUS_RAW, null, productDesc, longProductDesc);

            context.write(NullWritable.get(), ad);
            context.getCounter(ParserEnum.SUCCESS).increment(1);

        } catch (IOException ex) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
