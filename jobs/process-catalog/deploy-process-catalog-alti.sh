rm lib/*
cp  coord-config-default-alti.xml coord-config-default.xml
cp  ../../../kosei-management-backend/client/build/libs/management-client-*-standalone.jar ./lib/
cp  ../../build/libs/ad-creator-workflow-*-SNAPSHOT-all.jar ./lib/
hadoop fs -rm -R hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/process-catalog;hadoop fs -put ../process-catalog hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/
hadoop fs -chmod 777 hdfs://nn-foa.s3s.altiscale.com:8020/shared/kosei/adcreator/oozie/process-catalog/lib/*

