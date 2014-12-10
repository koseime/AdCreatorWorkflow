package com.kosei.adcreatorworkflow.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * Created by hess on 12/9/14.
 */
public class WorkflowTimestampProperties {

  public static void main(String[] args) throws IOException {
    String propertiesFile = System.getProperty("oozie.action.output.properties");
    if (propertiesFile != null) {
      File propFile = new File(propertiesFile);

      Properties properties = new Properties();
      Calendar date = Calendar.getInstance();
      properties.setProperty("TIMESTAMP", Long.toString(date.getTimeInMillis()));
      properties.setProperty("year", Integer.toString(date.get(Calendar.YEAR)));
      properties.setProperty("month", Integer.toString(date.get(Calendar.MONTH)));
      properties.setProperty("day", Integer.toString(date.get(Calendar.DAY_OF_MONTH)));
      properties.setProperty("hour", Integer.toString(date.get(Calendar.HOUR_OF_DAY)));
      properties.setProperty("minute", Integer.toString(date.get(Calendar.MINUTE)));
      properties.setProperty("second", Integer.toString(date.get(Calendar.SECOND)));
      properties.setProperty("millisecond", Integer.toString(date.get(Calendar.MILLISECOND)));

      OutputStream os = new FileOutputStream(propFile);
      properties.store(os, "");
      os.close();
    } else {
      throw new RuntimeException("oozie.action.output.properties not defined");
    }

  }
}
