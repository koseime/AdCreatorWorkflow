# argument 1: s3 access key
# argument 2: s3 secret key
# argument 3: destination base path
# argument 4: file of comma separated list of catalog locations
# argument 5: file of comma separated list of catalog names

command="hadoop fs -cat $4"
catalog_locations=$(eval $command)

command="hadoop fs -cat $5"
catalog_names=$(eval $command)

IFS="," catalog_names=($catalog_names)
IFS="," catalog_locations=($catalog_locations)

i=0
for catalog_location in ${catalog_locations[@]}
do
    command="hadoop distcp \"-Dfs.s3n.awsAccessKeyId=$1\" \"-Dfs.s3n.awsSecretAccessKey=$2\" \"$catalog_location\" \"$3/${catalog_names[$i]}\""
    # echo $command
    eval $command || exit 1
    i=`expr $i + 1`
done
