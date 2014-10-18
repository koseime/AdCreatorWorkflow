# argument 1: s3 access key
# argument 2: s3 secret key
# argument 3: destination base path
# argument 4: comma separated list of catalog locations

catalog_locations=$(echo $4 | tr "," "\n")

for catalog_location in $catalog_locations
do
    bucket_path=${catalog_location:6}
    s3n_full="s3n://$1:$2@$bucket_path"
    command="hadoop distcp $s3n_full $3"
    echo "$command"
    eval "$command"
done
