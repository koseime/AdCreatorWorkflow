package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import com.kosei.proto.AdComponentsMessages.AdComponents;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author root
 */
public class ByteWritableImageCrawlerMapper extends
        Mapper<NullWritable, AdCreatorAssetsWritable, NullWritable, BytesWritable> {

    private final static Logger LOG = Logger.getLogger(ByteWritableImageCrawlerMapper.class.getName());

    @Override
    public void map(NullWritable key, AdCreatorAssetsWritable value, Context context)

            throws IOException, InterruptedException {

        String uri = value.getThumbPicURI().toString();
        if (uri == null || uri.length() == 0) {
            uri = value.getLowPicURI().toString();
        }


        if (value.getStatus().get() == AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED) {
            LOG.log(Level.INFO, "Image Already retrieved ID:" + value.getId());
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
                LOG.log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, e);
                AdComponents.Builder adBuilder = AdComponents.newBuilder().setId(value.getId().toString())
                        .setDescription(value.getProductDesc().toString())
                        .setProductJpg(ByteString.copyFrom(new byte[0]))
                        .setStatus(AdComponents.Status.IMAGE_RETRIEVAL_FAILURE);
                putMeta(adBuilder, value.entrySetMeta());
                try {
                    BytesWritable bw = new BytesWritable(objectToByteBuffer(adBuilder.build()));
                    context.write(NullWritable.get(), bw);
                    return;
                } catch (Exception e2) {
                    LOG.log(Level.SEVERE, "Failed writable.. ID:" + value.getId() + " URL:" + uri, e2);
                    return;
                }
            }


            byte[] data = readFully(conn.getInputStream());
            value.setGeneratedJpgAd(data);
            value.setStatus(AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED);

            AdComponents.Builder adBuilder = AdComponents.newBuilder().setId(value.getId().toString())
                    .setDescription(value.getProductDesc().toString())
                    .setProductJpg(ByteString.copyFrom(data))
                    .setStatus(AdComponents.Status.IMAGE_RETRIEVED);
            //System.err.println(value.entrySetMeta().size());
            putMeta(adBuilder, value.entrySetMeta());
            //System.err.print(adBuilder.build().getMeta(0).getKey() + " ");
            //System.err.println(adBuilder.build().getMeta(0).getValue());
            try {
                BytesWritable bw = new BytesWritable(objectToByteBuffer(adBuilder.build()));
                context.write(NullWritable.get(), bw);
                return;
            } catch (Exception e2) {
                LOG.log(Level.SEVERE, "Failed writable.. ID:" + value.getId() + " URL:" + uri, e2);
                return;
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, ex);
        } catch (NullPointerException ex) {
            LOG.log(Level.SEVERE, "ID:" + value.getId() + " URL:" + uri, ex);
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

    public void putMeta(AdComponents.Builder adBuilder, Set<Map.Entry<Writable, Writable>> entries) {
        Iterator i = entries.iterator();
        while (i.hasNext()) {
            Map.Entry<Writable, Writable> keyValue = (Map.Entry<Writable, Writable>)i.next();
            Text key = (Text)keyValue.getKey();
            BytesWritable value = (BytesWritable)keyValue.getValue();

            AdComponents.Meta metaEntry = AdComponents.Meta.newBuilder()
                    .setKey(key.toString())
                    .setValue(ByteString.copyFrom(value.copyBytes()))
                    .build();
            //System.err.print(key.toString() + " ");
            //System.err.println(value.copyBytes());
            adBuilder.addMeta(metaEntry);
        }
    }
}
