package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;


import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author root
 */
public class ImageCrawlerMapper extends
        Mapper<NullWritable, AdCreatorAssetsWritable, NullWritable, ProtobufWritable<AdComponents>> {


    @Override
    public void map(NullWritable key, AdCreatorAssetsWritable value, Context context)

            throws IOException, InterruptedException {

        String uri = value.getImageURIs().toStrings()[0];

        if (value.getStatus().get()==AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED) {
            Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.INFO, "Image Already retrieved ID:" + value.getId());

        }
        try {
            String type = "";
            URLConnection conn;

            try {
                URL link = new URL(uri);
                conn = link.openConnection();
                conn.connect();
                type = conn.getContentType();
            } catch (Exception e) {
                Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, e);
                ProtobufWritable<AdComponents> out = ProtobufWritable.newInstance(AdComponents.class);
                out.set(AdComponents.newBuilder().setId(value.getId().toString())
                        .setDescription(value.getProductDesc().toString())
                        .setProductJpg(ByteString.copyFrom(new byte[0]))
                        .setStatus(AdComponents.Status.IMAGE_RETRIEVAL_FAILURE)
                        .build());
                context.write(NullWritable.get(), out);
                return;
            }

            byte[] data = readFully(conn.getInputStream());
            value.setImageBlob(data);
            value.setStatus(AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED);

            ProtobufWritable<AdComponents> out = ProtobufWritable.newInstance(AdComponents.class);
            out.set(AdComponents.newBuilder().setId(value.getId().toString())
                    .setDescription(value.getProductDesc().toString())
                    .setProductJpg(ByteString.copyFrom(data))
                    .setStatus(AdComponents.Status.IMAGE_RETRIEVED)
                    .build());
            context.write(NullWritable.get(), out);
        } catch (IOException ex) {
            Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId()+ " URL:" + uri, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId()+ " URL:" + uri, ex);
        }

    }

    public static byte[] readFully(InputStream input) throws IOException {
        byte[] buffer = new byte[8192];
        int bytesRead;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((bytesRead = input.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        return output.toByteArray();
    }

}
