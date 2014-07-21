package com.kosei.adcreatorworkflow.hadoop.io;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lanceriedel on 7/17/14.
 */
public class AdCreatorAssetsWritable implements Writable {
    public static final int STATUS_EMPTY = 0;
    public static final int STATUS_RAW = 1;
    public static final int STATUS_IMAGE_RETRIEVED = 2;

    private BytesWritable generatedJpgAd;

    private Text id, lowPicURI, thumbPicURI, productDesc, longProductDesc;
    private IntWritable status;

    public AdCreatorAssetsWritable() {
        this.id = new Text();
        this.productDesc = new Text();
        this.longProductDesc = new Text();
        this.lowPicURI = new Text();
        this.thumbPicURI = new Text();
        this.status = new IntWritable(STATUS_EMPTY);
        generatedJpgAd = null;
    }


    public AdCreatorAssetsWritable(Text id, Text lowPicURI, Text thumbPicURI, IntWritable status, BytesWritable generatedJpgAd,
                                   Text productDesc, Text longProductDesc) {
        this.id = id;
        this.productDesc = productDesc;
        this.longProductDesc = longProductDesc;
        this.thumbPicURI = thumbPicURI;
        this.lowPicURI = lowPicURI;
        this.status = status;
        this.generatedJpgAd = generatedJpgAd;
    }

    public AdCreatorAssetsWritable(String id, String lowPicURI, String thumbPicURI, int status, byte[] generatedJpgAd,
                                   String productDesc, String longProductDesc) {
        this.id = new Text(id);
        this.productDesc =  new Text(productDesc);
        this.longProductDesc =  new Text(longProductDesc);
        this.thumbPicURI =  new Text(thumbPicURI);
        this.lowPicURI =  new Text(lowPicURI);
        this.status = new IntWritable(status);
        if (generatedJpgAd==null)
            generatedJpgAd = new byte[0];
        this.generatedJpgAd = new BytesWritable(generatedJpgAd);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        productDesc.readFields(in);
        longProductDesc.readFields(in);
        lowPicURI.readFields(in);
        thumbPicURI.readFields(in);
        status.readFields(in);
        if (status.get()==1) {
            byte[] generatedJpgAd = new byte[0];
            this.generatedJpgAd = new BytesWritable(generatedJpgAd);
        } else {
            generatedJpgAd.readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        productDesc.write(out);
        longProductDesc.write(out);
        lowPicURI.write(out);
        thumbPicURI.write(out);
        status.write(out);
        generatedJpgAd.write(out);
    }


    public BytesWritable getGeneratedJpgAd() {
        return generatedJpgAd;
    }

    public void setGeneratedJpgAd(BytesWritable generatedJpgAd) {
        this.generatedJpgAd =  generatedJpgAd;
    }

    public void setGeneratedJpgAd(byte[] generatedJpgAd) {
        this.generatedJpgAd = new BytesWritable(generatedJpgAd);
    }

    public Text getId() {
        return id;
    }

    public void setStatus(int status) {
        this.status = new IntWritable(status);
    }

    public void setStatus(IntWritable status) {
        this.status = status;
    }

    public Text getLowPicURI() {
        return lowPicURI;
    }

    public Text getThumbPicURI() {
        return thumbPicURI;
    }

    public Text getProductDesc() {
        return productDesc;
    }

    public Text getLongProductDesc() {
        return longProductDesc;
    }

    public IntWritable getStatus() {
        return status;
    }
}
