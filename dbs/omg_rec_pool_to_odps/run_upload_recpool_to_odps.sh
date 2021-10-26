#!/usr/bin/env bash

export PYTHONPATH=../:../../

odpscmd="/home/work/omg-odpscmd/bin/odpscmd"
pythonenv="/usr/bin/python"


source ../../utils/errtrap.sh
trap 'ERRTRAP $LINENO "omg_recpool" "xiaofendui"' ERR


recpool_info_table_name="omg_data.omg_recpool_info"
${odpscmd} -e "
    create table if not exists ${recpool_info_table_name}
    (pid bigint, mid bigint, tid bigint, ptype bigint, gid bigint, ct bigint, rec bigint, download_num bigint,
     like_num bigint, score_mix double, gid_ct bigint, fans bigint, atts bigint, is_vulgar bigint, is_sexy bigint, choice_status bigint, choice_types string,
     vdur bigint, real_sume bigint, ctr double, play_rate double, dtr double, qscore double, age_tgi double, mover_level bigint, mover_part_id bigint, review_cnt bigint,
     community_tag_new bigint, community_tag_high bigint, community_pre_tag bigint, share_num bigint, step_boot bigint, step_boot_t bigint, community_tag_godrev bigint,
     subarea_status bigint, subarea_types string, like_bait bigint, resource_status bigint, subarea_time bigint, detail_cnt bigint, source string, original bigint,
     assist_status bigint, official_template_id bigint, community_hashtag_post bigint);
    TRUNCATE TABLE ${recpool_info_table_name}
"


recpool_data='./recpool.dat'
${pythonenv} clean_data.py --rec_pool=${recpool_data}

${odpscmd} -e "tunnel upload ${recpool_data} ${recpool_info_table_name} -fd '\t'"


recpool_table_name="omg_recpool"
${odpscmd} -e "
use omg_data;
drop table if exists $recpool_table_name;
ALTER TABLE $recpool_info_table_name RENAME TO $recpool_table_name;
"
