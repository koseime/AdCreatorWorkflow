cp  ../../../kosei-management-backend/client/build/libs/management-client-0.5-standalone.jar ./lib/
cp  ../../build/libs/ad-creator-workflow-0.1-SNAPSHOT-all.jar ./lib/
hadoop fs -rm -R kosei/adcreator/oozie/process-catalog;hadoop fs -put ../process-catalog kosei/adcreator/oozie/
