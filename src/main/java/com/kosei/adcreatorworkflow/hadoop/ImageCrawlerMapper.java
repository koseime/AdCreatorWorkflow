package com.kosei.adcreatorworkflow.hadoop;

import com.kosei.adcreatorworkflow.hadoop.io.AdCreatorAssetsWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author root
 */
public class ImageCrawlerMapper extends
        Mapper<NullWritable, AdCreatorAssetsWritable, NullWritable, AdCreatorAssetsWritable> {


    @Override
    public void map(NullWritable key, AdCreatorAssetsWritable value, Context context)

            throws IOException, InterruptedException {

        String uri = value.getThumbPicURI().toString();
        if (uri == null || uri.length() == 0) {
            uri = value.getLowPicURI().toString();
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
                Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId(), e);
                context.write(NullWritable.get(), value);
                return;
            }

            byte[] data = readFully(conn.getInputStream());
            value.setGeneratedJpgAd(data);
            value.setStatus(AdCreatorAssetsWritable.STATUS_IMAGE_RETRIEVED);

            context.write(NullWritable.get(), value);
        } catch (IOException ex) {
            Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId(), ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(ImageCrawlerMapper.class.getName()).log(Level.SEVERE, "ID:" + value.getId(), ex);
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
