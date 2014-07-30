cp  ../../target/hadoop-job-demo-1.0-SNAPSHOT-job.jar ./lib/
hadoop fs -rm -R kosei/adcreator/oozie/adcreatorworkflow;hadoop fs -put ../adcreatorworkflow kosei/adcreator/oozie/
