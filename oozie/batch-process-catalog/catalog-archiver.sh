# argument 1: s3 access key
# argument 2: s3 secret key
# argument 3: processed catalogs root
# argument 4: advertisers root

ls_command="hadoop fs -ls $3"
file_list=$(echo `eval $ls_command` | grep -o "$3[^[:space:]]*images[^[:space:]]*")

for file in $file_list
do
    filename=${file#$3"/"}
    IFS="-." parts=($filename)
    advertiser_id=${parts[0]}
    catalog_id=${parts[2]}
    dest="$4/$advertiser_id/catalog/archive/$catalog_id"
    eval "hadoop fs -mkdir -p $dest"
    eval "hadoop fs -mv $file $dest"
done

