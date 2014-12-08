# argument 1: s3 access key
# argument 2: s3 secret key
# argument 3: destination base path
# argument 4: file of comma separated list of catalog locations
# argument 5: file of comma separated list of catalog names

command="hadoop fs -cat $4"
catalog_locations=$(eval $command)

command="hadoop fs -cat $5"
catalog_names=$(eval $command)

IFS=$'\n' catalog_names=($catalog_names)
IFS=$'\n' catalog_locations=($catalog_locations)

command="hadoop distcp \"-Dfs.s3n.awsAccessKeyId=$1\" \"-Dfs.s3n.awsSecretAccessKey=$2\" \"-f $4\" \"$3\""
eval $command || exit 1

i=0
for catalog_location in ${catalog_locations[@]}
do
    filename=`basename $catalog_location`
    command="hadoop fs -mv \"$3/$filename\" \"$3/${catalog_names[$i]}\""
    eval $command || exit 1
    i=`expr $i + 1`
done
