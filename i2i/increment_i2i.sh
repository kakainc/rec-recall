#!/usr/bin/env bash

today=`date +%Y%m%d`
max_day=10
model_dir="/data/work/recoffline/i2i/models"

if [ ! -d ${model_dir} ];then
  mkdir ${model_dir}
fi

action=$1

# 对目录下的更改时间在10天前的普通文件执行删除操作
count=`ls ${model_dir} | wc -w`
if [ "$count" -gt "0" ]; then
  find ${model_dir} -name "*${action}*" -type f -mtime +${max_day} -exec rm {} \;
fi

increment="1"

hour=$(date "+%H")

increment_tag_file=${model_dir}"/${action}_${today}.done"
if [[ ${hour}  -ge "03" ]] && [ ! -f "${increment_tag_file}" ]
then
    increment="0"
fi


echo "start train ${action}"
bash -x train_i2i_model.sh "${action}" ${increment}
echo "end train ${action}"

