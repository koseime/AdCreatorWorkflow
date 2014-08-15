source /etc/profile

# This script does the following.
# Compare the file counts on S3 and HDFS to make sure distcp pulled everything.

export JAVA_HOME=/usr/java/default

SOURCE_DIR=$1
TARGET_DIR=$2

echo "Source Folder: $SOURCE_DIR"

echo "Target Folder: $TARGET_DIR"

# Compare counts and send appropriate mails.

hdp_count=$(/opt/hadoop/bin/hadoop fs -ls $TARGET_DIR | wc -l)
s3_count=$(/opt/hadoop/bin/hadoop fs -conf s3-credentials.xml -ls $SOURCE_DIR | wc -l)

echo "ALTISCALE_COUNT: $hdp_count"
echo "S3_COUNT: $s3_count"

if [ "$hdp_count" == "$s3_count" ]; then
    echo "SUCCESS = TRUE"
else 
    echo "SUCCESS = FALSE"
fi

exit 0

