package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import javax.ws.rs.core.Context;
import java.io.*;
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

    private String catalogHeader = "";
    private GoogleProductItemParser googleProductItemParser = null;
    private byte[] timestampBytes = null;

    @Override
    public void setup(Context context) {
        String catalogHeaderFilePath = context.getConfiguration().get("catalog.header.file");
        try {
            catalogHeader = getCatalogHeader(catalogHeaderFilePath);
        } catch (Exception e) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, e);
        }
        googleProductItemParser = new GoogleProductItemParser(catalogHeader);

        String timestampString = context.getConfiguration().get("catalog.timestamp");
        long timestamp = Long.parseLong(timestampString);
        timestampBytes = ByteBuffer.allocate(8).putLong(timestamp).array();
    }

    @Override
    public void map(LongWritable key, Text value1, Context context)
            throws IOException, InterruptedException {
        String googleCatRecord = value1.toString();
        if (googleCatRecord.equals(catalogHeader)) { return; }
        context.getCounter(ParserEnum.INPUTREC).increment(1);

        try {
            GoogleProductItem gpi = googleProductItemParser.parse(googleCatRecord);

            String productId = gpi.getId();
            String imageURI = gpi.getImageLink();
            String additionalImageURIs = gpi.getAdditionalImageLink();
            String productDesc = gpi.getTitle();
            String longProductDesc = gpi.getDescription();
            String category = cleanCategory(gpi.getGoogleProductCategory());
            String availability = gpi.getAvailability();

            if (productId.isEmpty()) { return; }
            AdCreatorAssetsWritable ad = new AdCreatorAssetsWritable(productId,
                    GoogleProductItemParser.getAllImageURIs(imageURI, additionalImageURIs),
                    AdCreatorAssetsWritable.STATUS_RAW, null, productDesc, longProductDesc);
            ad.putMeta(new Text("timestamp"), new BytesWritable(timestampBytes));

            if (availability.equals("out of stock")) {
                ad.putMeta(new Text("category"), new BytesWritable("DELETED".getBytes()));
                ad.putMeta(new Text("deleted"), new BytesWritable(availability.getBytes()));
            } else {
                ad.putMeta(new Text("category"), new BytesWritable(category.getBytes()));
            }

            context.write(new Text(""), ad);
            context.write(new Text(ad.getId()), ad);
            context.getCounter(ParserEnum.SUCCESS).increment(1);

        } catch (Exception e) {
            Logger.getLogger(GoogleTextParserMapper.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    private String getCatalogHeader(String filename) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String catalogHeader = br.readLine();
        br.close();
        return catalogHeader;
    }

    private String cleanCategory(String category) {
        category = category.replaceAll("(\\s*)>(\\s*)", ">");
        category = category.trim();
        return category;
    }

}
