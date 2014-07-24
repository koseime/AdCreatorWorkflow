package com.kosei.adcreatorworkflow.hadoop;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.kosei.proto.AdComponentsMessages;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by lanceriedel on 7/23/14.
 */
public class ProtobufTest {
    @Test
    public void test() {
        AdComponentsMessages.AdComponents ad = AdComponentsMessages.AdComponents.newBuilder().setId("1234")
                .setDescription("Junk description")
                .setProductJpg(ByteString.copyFrom(new byte[0]))
                .setStatus(AdComponentsMessages.AdComponents.Status.IMAGE_RETRIEVAL_FAILURE)
                .build();
        try {

            FileOutputStream os = new FileOutputStream(new File("/tmp/orig.dat"));
            os.write(objectToByteBuffer(ad));
            os.close();
            BytesWritable bw = new BytesWritable(objectToByteBuffer(ad));


            System.out.println("out:" + bw);
            return;
        } catch (Exception e2) {
            e2.printStackTrace();
            return;
        }


    }

    public byte[] objectToByteBuffer(Object o) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Message message = (Message) o;

        String messageAsString = message.toString();
        return messageAsString.getBytes();
//        baos.write(messageAsString.);
//        return baos.toByteArray();
    }
}
