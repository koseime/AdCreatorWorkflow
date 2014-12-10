package com.kosei.adcreatorworkflow.hadoop.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kosei.dropwizard.management.api.Page;
import com.kosei.dropwizard.management.api.Resource;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hess on 12/9/14.
 */
public abstract class BaseRestResourceMapper<T extends Resource>  extends
                                     Mapper<NullWritable, NullWritable, LongWritable, BytesWritable> {

  public static enum RestResourceCounters {GOOD_RECORDS, BAD_RECORDS};
  public static final int PAGE_SIZE = 100;
  private static final int MAX_RECORDS = Integer.MAX_VALUE;


  protected abstract Page<T> nextPage(long index);
  protected abstract ObjectMapper getObjectMapper();
  protected abstract long getIdForObject(T object);

  @Override
  protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
    int localCounter = 0;


    Page<T> catalogVersionsPage;
    long index = 0;
    do {

      catalogVersionsPage = nextPage(index);
      for(T cv : catalogVersionsPage.content) {
        try {
          localCounter++;
          context.getCounter(RestResourceCounters.GOOD_RECORDS).increment(1L);
          context.write(new LongWritable(getIdForObject(cv)), new BytesWritable(getObjectMapper().writeValueAsBytes(cv)));
        } catch (IOException e) {
          context.getCounter(RestResourceCounters.BAD_RECORDS).increment(1L);
        } catch (InterruptedException e) {
          context.getCounter(RestResourceCounters.BAD_RECORDS).increment(1L);
        }
      }

      index += PAGE_SIZE;
    } while(catalogVersionsPage != null && catalogVersionsPage.getNextLink() != null && localCounter < MAX_RECORDS) ;

  }

}
