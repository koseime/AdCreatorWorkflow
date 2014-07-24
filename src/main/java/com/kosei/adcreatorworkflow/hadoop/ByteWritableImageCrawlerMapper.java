package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author root
 */
public class ByteWritableImageCrawlerMapper extends
        Mapper<NullWritable, AdCreatorAssetsWritable, NullWritable, BytesWritable> {


    @Override
    public void map(NullWritable key, AdCreatorAssetsWritable value, Context context)

            throws IOException, InterruptedException {

        String uri = value.getThumbPicURI().toString();
        if (uri == null || uri.length() == 0) {
            uri = value.getLowPicURI().toString();
        }


        if (value.getStatus().get() == AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED) {
            Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.INFO, "Image Already retrieved ID:" + value.getId());

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
                Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, e);
                AdComponents ad = AdComponents.newBuilder().setId(value.getId().toString())
                        .setDescription(value.getProductDesc().toString())
                        .setProductJpg(ByteString.copyFrom(new byte[0]))
                        .setStatus(AdComponents.Status.IMAGE_RETRIEVAL_FAILURE)
                        .build();
                try {
                    BytesWritable bw = new BytesWritable(objectToByteBuffer(ad));
                    context.write(NullWritable.get(), bw);
                    return;
                } catch (Exception e2) {
                    Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.SEVERE, "Failed writable.. ID:" + value.getId() + " URL:" + uri, e2);
                    return;
                }
            }


            byte[] data = readFully(conn.getInputStream());
            value.setGeneratedJpgAd(data);
            value.setStatus(AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED);

            AdComponents ad = AdComponents.newBuilder().setId(value.getId().toString())
                    .setDescription(value.getProductDesc().toString())
                    .setProductJpg(ByteString.copyFrom(data))
                    .setStatus(AdComponents.Status.IMAGE_RETRIEVED)
                    .build();
            try {
                BytesWritable bw = new BytesWritable(objectToByteBuffer(ad));
                context.write(NullWritable.get(), bw);
                return;
            } catch (Exception e2) {
                Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.SEVERE, "Failed writable.. ID:" + value.getId() + " URL:" + uri, e2);
                return;
            }
        } catch (IOException ex) {
            Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, ex);
        }

    }


    public byte[] objectToByteBuffer(Object o) throws Exception {
        Message message = (Message) o;

        byte[] messageBytes = message.toByteArray();
        return messageBytes;
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
