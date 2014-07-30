cp  ../../build/libs/ad-creator-workflow-0.1-SNAPSHOT-all.jar ./lib
hadoop fs -rm -R kosei/adcreator/oozie/adcreatorworkflow;hadoop fs -put ../adcreatorworkflow kosei/adcreator/oozie/
