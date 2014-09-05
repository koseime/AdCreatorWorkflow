rm lib/*
cp coord-config-default-alti.xml coord-config-default.xml
cp  ../../../kosei-management-backend/client/build/libs/management-client-*-standalone.jar ./lib/
cp ../../../AdCreator/ad-creator bin/ad-creator
cp  ../../build/libs/ad-creator-workflow-0.1-SNAPSHOT-all.jar ./lib
hadoop fs -rm -R hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/create-campaign;hadoop fs -put ../create-campaign hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie
hadoop fs -chmod 777 hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/create-campaign/lib/*
hadoop fs -chmod 777 hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/create-campaign/bin/*
