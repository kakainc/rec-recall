#!/usr/bin/env bash

export PYTHONPATH=../:../../
pythonenv="/usr/bin/python3"

base_path="/data/work/recoffline/i2i"
model_dir="/data/work/recoffline/i2i/models"

action=$1
increment=$2

# 整理训练数据
today=`date +%Y%m%d`
yesterday=`date -d -1day +%Y%m%d`
max_day=30
if [ ${action} == "click" ]; then
  max_day=7
fi

if [[ ${action} =~ "no_increment" ]]; then
  increment="0"
  yesterday=`date +%Y%m%d`
  max_day=0
fi

datadir="${base_path}/${action}/data"
if [ ! -d ${datadir} ];then
  mkdir -p ${datadir}
fi

sequence_script="${base_path}/${action}_pids.sh"
if [ ${increment} == "0" ]; then
    datapath="${datadir}/${action}_${yesterday}"
    cd "${base_path}/${action}" && sh ${sequence_script} ${yesterday} ${datapath}
else
    datapath="${datadir}/${action}_${today}"
    cd "${base_path}/${action}" && sh ${sequence_script} ${today} ${datapath}
fi

train_file=${datapath}
if [ ${increment} == "0" ]; then
    file_list=()
    for file_name in ${datadir}/*; do
        file_date=`echo ${file_name} |awk -F'_' '{print $NF}'`
        diff_day=$((($(date +%s -d ${yesterday}) - $(date +%s -d ${file_date}))/86400))
        if [ ${diff_day} -gt ${max_day} ];then
            rm ${file_name}
            echo "rm ${file_name}"
        else
            file_list=(${file_list[@]} ${file_name})
        fi
    done

    files=""
    for item in ${file_list[@]}
    do
        files=${files}" "${item}
    done

    train_file="${base_path}/${action}/lastest_seq.dat"
    cat ${files} > ${train_file}
fi

# 训练模型
train_script="/data/work/recoffline/i2i/train_w2v.py"
start_time=`date +"%Y-%m-%d %k:%M:%S"`
start_time_unix=`date +%s`
echo "train, start: ${start_time}"

if [ ${action} == "review" ]; then
    if [ ${increment} == "0" ]; then
        model_path="${model_dir}/${action}.model.dat.${yesterday}"
        old_model_path="NULL"
    else
        model_path="${model_dir}/${action}.model.dat.`date '+%Y%m%d'`"
        old_model_path="${model_dir}/${action}.model.dat.${yesterday}"
    fi
    model_format="word2vec"
    ${pythonenv} ${train_script} ${train_file} ${model_path} ${old_model_path}
fi

end_time1=`date +"%Y-%m-%d %k:%M:%S"`
end_time1_unix=`date +%s`
spent=$(($end_time1_unix - $start_time_unix))
echo "train, end: ${end_time1}"
echo "train, spend time: ${spent}"

# 生成每个pid相近向量
video_output_path=${base_path}"/${action}_video_i2i.txt"
imgtext_output_path=${base_path}"/${action}_img_i2i.txt"

cd ${base_path}
post_ptype_tid_file_path="${action}_post_ptype_tid.csv"
${pythonenv} gen_i2i_vecs.py ${model_path} ${video_output_path} ${imgtext_output_path} ${post_ptype_tid_file_path} --mtype ${model_format}
end_time2=`date +"%Y-%m-%d %k:%M:%S"`
end_time2_unix=`date +%s`
spent=$(($end_time2_unix - $end_time1_unix))
echo "gen_i2i, end: ${end_time2}"
echo "gen_i2i, spend time: ${spent}"


if [ ${action} == "review" ]; then
    ${pythonenv} index_i2i_review.py ${video_output_path} index_${action}_i2i_video_ video ${post_ptype_tid_file_path}
    ${pythonenv} index_i2i_review.py ${imgtext_output_path} index_${action}_i2i_imgtext_ img ${post_ptype_tid_file_path}
elif [ ${action} == "status_tab_click" ]; then
    ${pythonenv} index_i2i.py ${video_output_path} index_${action}_i2i_video_
    ${pythonenv} index_i2i.py ${imgtext_output_path} index_${action}_i2i_imgtext_
else
    # 分男女打索引
    ${pythonenv} index_i2i_gender_concurrence.py ${video_output_path} index_${action}_source_i2i_video_ video ${post_ptype_tid_file_path}
    ${pythonenv} index_i2i_gender_concurrence.py ${imgtext_output_path} index_${action}_source_i2i_imgtext_ img ${post_ptype_tid_file_path}
fi

# 生成index时不过滤帖子
if [ ${action} == "review" ] || [ ${action} == "download" ] || [ ${action} == "like" ]; then
    ${pythonenv} index_i2i.py ${video_output_path} index_${action}_v2_i2i_video_ video ${post_ptype_tid_file_path}
    ${pythonenv} index_i2i.py ${imgtext_output_path} index_${action}_v2_i2i_imgtext_ img ${post_ptype_tid_file_path}
fi

if [ ${increment} == "0" ]; then
    touch ${model_dir}"/${action}_${today}.done"
fi

end_time3=`date +"%Y-%m-%d %k:%M:%S"`
end_time3_unix=`date +%s`
spent=$(($end_time3_unix - $end_time2_unix))
echo "index_i2i, end: ${end_time3}"
echo "index_i2i, spend time: ${spent}"
