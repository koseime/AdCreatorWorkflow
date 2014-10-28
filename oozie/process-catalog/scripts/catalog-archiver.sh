# argument 1: processed catalogs root
# argument 2: advertisers root

ls_command="hadoop fs -ls $1"
file_list=$(echo `eval $ls_command` | grep -o "$1[^[:space:]]*images-m-00000[^[:space:]]*")

for file in $file_list
do
    filename=${file#$1"/"}
    IFS="-." parts=($filename)
    advertiser_id=${parts[0]}
    catalog_id=${parts[2]}
    dest="$2/$advertiser_id/catalog/archive/$catalog_id"
    trigger_file="${filename%images-m-00000}READY"
    files="${file%-00000}*"    
    eval "hadoop fs -mkdir -p $dest"
    eval "hadoop fs -mv $files $dest"
    eval "hadoop fs -mkdir $dest/$trigger_file"
done

