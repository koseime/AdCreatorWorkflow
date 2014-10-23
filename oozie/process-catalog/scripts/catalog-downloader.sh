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
    bucket_path=${catalog_location:6}
    s3n_full="s3n://$1:$2@$bucket_path"
    # echo "hadoop distcp $s3n_full $3/${catalog_names[$i]}"
    eval "hadoop distcp $s3n_full $3/${catalog_names[$i]}"
    i=`expr $i + 1`
done
