# argument 1: s3 access key
# argument 2: s3 secret key
# argument 3: destination base path
# argument 4: comma separated list of catalog locations
# argument 5: comma separated list of catalog names

IFS="," catalog_names=($5)
IFS="," catalog_locations=($4)

i=0
for catalog_location in ${catalog_locations[@]}
do
    command="hadoop distcp -Dfs.s3n.awsAccessKeyId=$1 -Dfs.s3n.awsSecretAccessKey=$2 $catalog_location $3/${catalog_names[$i]}"
    # echo $command
    eval $command || exit 1
    i=`expr $i + 1`
done
