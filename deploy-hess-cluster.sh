#!/bin/zsh
set -x

NAMENODE_HOST=namenode.internal.kosei.me
NAMENODE_UNIXUSER=kosei
KEYFILE=~/.ssh/kosei-eastcoastpem.pem
HDFS_BASEDIR=/user/jonathan/testing

#NAMENODE_HOST=dell3.hessinteractive.local
#NAMENODE_UNIXUSER=jonathan
#KEYFILE=~/.ssh/id_rsa
#HDFS_BASEDIR=/user/jonathan/testing

function die {
    echo "$@"
    exit 1;
}

ssh -i $KEYFILE $NAMENODE_UNIXUSER@$NAMENODE_HOST 'rm -rf ~/hadoop ; mkdir ~/hadoop '
scp -i $KEYFILE  build/libs/ad-creator-workflow-*-all.jar "$NAMENODE_UNIXUSER@$NAMENODE_HOST:~/hadoop/ad-creator-workflow.jar"

# Step 1 - get catalog updates
ssh -i $KEYFILE $NAMENODE_UNIXUSER@$NAMENODE_HOST "cd ~/hadoop ; HADOOP_CLASSPATH=ad-creator-workflow.jar HADOOP_USER_NAME=jonathan hadoop com.kosei.adcreatorworkflow.hadoop.catalogs.FetchCatalogUpdates $HDFS_BASEDIR" || die "Failed FetchCatalogUpdates"

# Step 2 - download files from s3
ssh -i $KEYFILE $NAMENODE_UNIXUSER@$NAMENODE_HOST "cd ~/hadoop ; HADOOP_CLASSPATH=ad-creator-workflow.jar HADOOP_USER_NAME=jonathan hadoop com.kosei.adcreatorworkflow.hadoop.catalogs.ParseCatalogs $HDFS_BASEDIR" || die "Failed ParseCatalogs"

# Step 3 - Put results to hbase
ssh -i $KEYFILE $NAMENODE_UNIXUSER@$NAMENODE_HOST "cd ~/hadoop ; HADOOP_CLASSPATH=ad-creator-workflow.jar HADOOP_USER_NAME=jonathan hadoop com.kosei.adcreatorworkflow.hadoop.catalogs.CatalogToHbase $HDFS_BASEDIR" || die "Failed CatalogToHbase"
