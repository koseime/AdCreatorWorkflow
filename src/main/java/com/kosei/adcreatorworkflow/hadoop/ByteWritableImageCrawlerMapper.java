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

        if (value.getStatus().get() == AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED) {
            LOG.log(Level.INFO, "Image Already retrieved ID:" + value.getId());
        }

        String[] imageURIs = value.getImageURIs().toStrings();
        for (int i = 0; i < imageURIs.length; i++) {
            String uri = imageURIs[i];
            try {
                URL link = new URL(uri);
                URLConnection conn = link.openConnection();
                conn.connect();
                String type = conn.getContentType();

                byte[] data = readFully(conn.getInputStream());
                value.setImageBlob(data);
                value.setStatus(AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED);

                BytesWritable bw = buildAdComponentsBytesWritable(value, AdComponents.Status.IMAGE_RETRIEVED);
                context.write(NullWritable.get(), bw);
                return;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Image retrieval failure for ID: " + value.getId() + " URL: " + uri, e);
            }
        }

        value.setImageBlob(new byte[0]);
        BytesWritable bw = buildAdComponentsBytesWritable(value, AdComponents.Status.IMAGE_RETRIEVAL_FAILURE);
        context.write(NullWritable.get(), bw);
    }

    private BytesWritable buildAdComponentsBytesWritable(AdCreatorAssetsWritable adCreatorAssetsWritable,
                                           AdComponents.Status status) {
        AdComponents.Builder adBuilder = AdComponents.newBuilder().setId(adCreatorAssetsWritable.getId().toString())
                .setTitle(adCreatorAssetsWritable.getProductDesc().toString())
                .setDescription(adCreatorAssetsWritable.getLongProductDesc().toString())
                .setProductJpg(ByteString.copyFrom(adCreatorAssetsWritable.getImageBlob().copyBytes()))
                .setStatus(status);
        putMeta(adBuilder, adCreatorAssetsWritable.entrySetMeta());

        return new BytesWritable(objectToByteBuffer(adBuilder.build()));
    }

    private void putMeta(AdComponents.Builder adBuilder, Set<Map.Entry<Writable, Writable>> entries) {
        Iterator i = entries.iterator();
        while (i.hasNext()) {
            Map.Entry<Writable, Writable> keyValue = (Map.Entry<Writable, Writable>)i.next();
            Text key = (Text)keyValue.getKey();
            BytesWritable value = (BytesWritable)keyValue.getValue();

            AdComponents.Meta metaEntry = AdComponents.Meta.newBuilder()
                    .setKey(key.toString())
                    .setValue(ByteString.copyFrom(value.copyBytes()))
                    .build();
            adBuilder.addMeta(metaEntry);
        }
    }

    public byte[] objectToByteBuffer(Object o) {
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
