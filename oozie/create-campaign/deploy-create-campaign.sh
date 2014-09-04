cp  ../../../kosei-management-backend/client/build/libs/management-client-0.5-standalone.jar ./lib/
cp ../../../AdCreator/ad-creator bin/ad-creator
cp  ../../build/libs/ad-creator-workflow-0.1-SNAPSHOT-all.jar ./lib
hadoop fs -rm -R kosei/adcreator/oozie/create-campaign;hadoop fs -put ../create-campaign kosei/adcreator/oozie/
