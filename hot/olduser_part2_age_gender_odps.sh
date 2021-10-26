#!/usr/bin/env bash

export PYTHONPATH=../:../../

odpscmd="/home/work/odpscmd/bin/odpscmd"
pythonenv="/usr/bin/python3"

day=`date -d "7 days ago" +%Y%m%d`

post_data_table="olduser_hot_part2_age_gender"
video_table="olduser_video_hot_part2_age_gender"
img_table="olduser_img_hot_part2_age_gender"


sql="
DROP TABLE IF EXISTS ${post_data_table};
CREATE TABLE ${post_data_table} as
SELECT *, play_video_dur/(CASE WHEN vdur<10 and vdur > 0 THEN 10 when vdur=0 then 900 ELSE COALESCE(vdur, 900) END) AS play_rate
FROM (
            SELECT
                    a.pid
                    ,NVL(part2_id, 72) as part2_id
                    ,nvl(c.age, 0) as group_age
                    ,nvl(c.gender, 0) as group_gender
                    ,CASE WHEN a.ptype != 5 THEN 2 ELSE a.ptype END AS ptype
                    ,max(case when b.vdur > 0 then b.vdur else a.vdur end) AS vdur
                    ,SUM(expose) AS expose
                    ,SUM(if(play_video_dur>=5 or stay_img_dur>=2 or view_img>0, 1, 0))/(SUM(expose) + 200) AS ctr
                    ,SUM(if(share>0,1,0))/(SUM(expose) + 200) AS share_rate
                    ,SUM(if(download>0,1,0))/(SUM(expose) + 200) AS download_rate
                    ,SUM(if(stay_time>300, 300, stay_time))/(SUM(expose) + 200) AS dur
                    ,SUM(like)/(SUM(expose) + 200) AS like_rate
                    ,SUM(detail_post)/(SUM(expose) + 200) AS detail
                    ,SUM(if(detail_post_dur>600, 600, detail_post_dur))/(SUM(expose) + 200) AS detail_dur
                    ,SUM(if(play_video_dur>300, 300, play_video_dur))/(SUM(expose) + 200) AS play_video_dur
                    ,SUM(if(create_review+reply_review>2, 2, create_review+reply_review))/(SUM(expose) + 200) AS review_rate
                    ,SUM(reviews_review)/(SUM(expose) + 200) as review_get_rate
                    ,SUM(like_review)/(SUM(expose) + 200) as like_review_rate
                    ,SUM(if(view_img_dur>300, 300, view_img_dur))/(SUM(expose) + 200) AS view_img_dur
                    ,SUM(view_img)/(SUM(expose) + 200) AS view_img
                    ,SUM(CASE WHEN play_video_dur>5 THEN 1 ELSE 0 END)/(SUM(expose) + 200) AS play_video
                    ,SUM(if(view_comment_dur>600, 600,  view_comment_dur))/(SUM(expose) + 200) AS comment_dur
            FROM    user_post_score a
            inner join recpool e
            on a.pid = e.pid
            left outer join postmetadata b
            on a.pid = b.pid
            left join usermetadata c
	        on a.mid = c.mid
	        left join tid_partition_metadata d
            on b.tid = d.tid
            WHERE   ymd >= '${day}'
            AND     expose >= 1
            AND     to_char(from_unixtime(e.ct), 'yyyymmdd') >= '${day}'
            AND     e.review_cnt >= 20
            AND     e.pid is not null
            AND     e.rec >= 1000
            AND     ((e.ctr>=0.3 and a.ptype != 5) or (e.ctr>=0.4 and a.ptype = 5))
            AND     e.review_cnt/e.rec>=0.001
            GROUP BY a.pid, NVL(d.part2_id, 72), nvl(c.age, 0), nvl(c.gender, 0), CASE WHEN a.ptype != 5 THEN 2 ELSE a.ptype END
        ) s
"


sql_video="
DROP TABLE IF EXISTS ${video_table};
CREATE TABLE ${video_table} as
SELECT *, ROW_NUMBER() OVER(partition by group_gender, group_age, part2_id ORDER BY recall_score DESC) AS reviewed_rank
FROM (
      SELECT
          group_gender
          ,group_age
          ,part2_id
          ,pid
          ,round(case
          when group_gender='0' and group_age='1' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='0' and group_age='2' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='0' and group_age='3' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='0' and group_age='4' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='0' and group_age='0' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)

          when group_gender='1' and group_age='1' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='1' and group_age='2' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='1' and group_age='3' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='1' and group_age='4' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='1' and group_age='0' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)

          when group_gender='2' and group_age='1' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='2' and group_age='2' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='2' and group_age='3' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='2' and group_age='4' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          when group_gender='2' and group_age='0' then (sume_norm*0.32 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.3 + detail_dur_norm*0.2 + play_rate_norm*0.3 + play_video_norm*0.3 + review_rate_norm*0.4 + detail_norm*0.4)
          end, 4) as recall_score
          ,sume_norm
          ,like_norm
          ,share_norm
          ,download_norm
          ,dur_norm
          ,detail_dur_norm
          ,play_rate_norm
          ,play_video_norm
          ,review_rate_norm
          ,like_review_rate_norm
          ,review_get_rate_norm
          ,detail_norm
          ,comment_dur_norm
      FROM (
            SELECT  pid
                    ,ptype
                    ,part2_id
                    ,group_age
                    ,group_gender
                    ,CASE WHEN sume_norm>1 THEN 1 WHEN sume_norm<0 THEN 0 ELSE sume_norm END sume_norm
                    ,CASE WHEN like_norm>1 THEN 1 WHEN like_norm<0 THEN 0 ELSE like_norm END like_norm
                    ,CASE WHEN share_norm>1 THEN 1 WHEN share_norm<0 THEN 0 ELSE share_norm END share_norm
                    ,CASE WHEN download_norm>1 THEN 1 WHEN download_norm<0 THEN 0 ELSE download_norm END download_norm
                    ,CASE WHEN dur_norm>1 THEN 1 WHEN dur_norm<0 THEN 0 ELSE dur_norm END dur_norm
                    ,CASE WHEN detail_dur_norm>1 THEN 1 WHEN detail_dur_norm<0 THEN 0 ELSE detail_dur_norm END detail_dur_norm
                    ,CASE WHEN play_rate_norm>1 THEN 1 WHEN play_rate_norm<0 THEN 0 ELSE play_rate_norm END play_rate_norm
                    ,CASE WHEN play_video_norm>1 THEN 1 WHEN play_video_norm<0 THEN 0 ELSE play_video_norm END play_video_norm
                    ,CASE WHEN review_rate_norm>1 THEN 1 WHEN review_rate_norm<0 THEN 0 ELSE review_rate_norm END review_rate_norm
                    ,CASE WHEN like_review_rate_norm>1 THEN 1 WHEN like_review_rate_norm<0 THEN 0 ELSE like_review_rate_norm END like_review_rate_norm
                    ,CASE WHEN review_get_rate_norm>1 THEN 1 WHEN review_get_rate_norm<0 THEN 0 ELSE review_get_rate_norm END review_get_rate_norm
                    ,CASE WHEN detail_norm>1 THEN 1 WHEN detail_norm<0 THEN 0 ELSE detail_norm END detail_norm
                    ,CASE WHEN comment_dur_norm>1 THEN 1 WHEN comment_dur_norm<0 THEN 0 ELSE comment_dur_norm END comment_dur_norm
                    ,expose
            FROM    (
                        SELECT  rg.pid
                                ,rg.ptype
                                ,rg.part2_id
                                ,rg.group_age
                                ,rg.group_gender
                                ,COALESCE((ctr - ctr_min) / NULLIF((case when ctr_max>0 then ctr_max else 1 end - ctr_min),0), 0) sume_norm
                                ,COALESCE((like_rate - like_rate_min) / NULLIF((case when like_rate_max>0 then like_rate_max else 1 end - like_rate_min),0), 0) like_norm
                                ,COALESCE((share_rate - share_rate_min) / NULLIF((case when share_rate_max>0 then share_rate_max else 1 end - share_rate_min),0), 0) share_norm
                                ,COALESCE((download_rate - download_rate_min) / NULLIF((case when download_rate_max>0 then download_rate_max else 1 end - download_rate_min),0), 0) download_norm
                                ,COALESCE((dur - dur_min) / NULLIF((case when dur_max>0 then dur_max else 1 end - dur_min),0), 0) dur_norm
                                ,COALESCE((detail_dur - detail_dur_min) / NULLIF((case when detail_dur_max>0 then detail_dur_max else 1 end - detail_dur_min),0), 0) detail_dur_norm
                                ,COALESCE((play_rate - play_rate_min) / NULLIF((case when play_rate_max>0 then play_rate_max else 1 end - play_rate_min),0), 0) play_rate_norm
                                ,COALESCE((play_video - play_video_min) / NULLIF((case when play_video_max>0 then play_video_max else 1 end - play_video_min),0), 0) play_video_norm
                                ,COALESCE((review_rate - review_rate_min) / NULLIF((case when review_rate_max>0 then review_rate_max else 1 end - review_rate_min),0), 0) review_rate_norm
                                ,COALESCE((like_review_rate - like_review_rate_min) / NULLIF((case when like_review_rate_max>0 then like_review_rate_max else 1 end - like_review_rate_min),0), 0) like_review_rate_norm
                                ,COALESCE((review_get_rate - review_get_rate_min) / NULLIF((case when review_get_rate_max>0 then review_get_rate_max else 1 end - review_get_rate_min),0), 0) review_get_rate_norm
                                ,COALESCE((detail - detail_min) / NULLIF((case when detail_max>0 then detail_max else 1 end - detail_min),0), 0) detail_norm
                                ,COALESCE((comment_dur - comment_dur_min) / NULLIF((case when comment_dur_max>0 then comment_dur_max else 1 end - comment_dur_min),0), 0) comment_dur_norm
                                ,rg.expose
                        FROM ${post_data_table} rg
                        LEFT OUTER JOIN (
                                SELECT
                                    ctr_max,like_rate_max,share_rate_max,download_rate_max,dur_max,detail_dur_max,play_video_max,play_rate_max,review_rate_max,like_review_rate_max,review_get_rate_max,detail_max,comment_dur_max
                                    ,case when ctr_min>0 then ctr_min else 0 end as ctr_min
                                    ,case when like_rate_min>0 then like_rate_min else 0 end as like_rate_min
                                    ,case when share_rate_min>0 then share_rate_min else 0 end as share_rate_min
                                    ,case when download_rate_min>0 then download_rate_min else 0 end as download_rate_min
                                    ,case when dur_min>0 then dur_min else 0 end as dur_min
                                    ,case when detail_dur_min>0 then detail_dur_min else 0 end as detail_dur_min
                                    ,case when play_rate_min>0 then play_rate_min else 0 end as play_rate_min
                                    ,case when play_video_min>0 then play_video_min else 0 end as play_video_min
                                    ,case when review_rate_min>0 then review_rate_min else 0 end as review_rate_min
                                    ,case when like_review_rate_min>0 then like_review_rate_min else 0 end as like_review_rate_min
                                    ,case when review_get_rate_min>0 then review_get_rate_min else 0 end as review_get_rate_min
                                    ,case when detail_min>0 then detail_min else 0 end as detail_min
                                    ,case when comment_dur_min>0 then comment_dur_min else 0 end as comment_dur_min
                                    ,ptype
                                    ,part2_id
                                    ,group_age
                                    ,group_gender
                                from
                                (
                                    SELECT
                                         percentile(ctr*100000, 0.15)/100000 - 1.5* (percentile(ctr*100000, 0.95)/100000-percentile(ctr*100000, 0.15)/100000) ctr_min
                                        ,percentile(ctr*100000, 0.95)/100000 + 1* (percentile(ctr*100000, 0.95)/100000-percentile(ctr*100000, 0.15)/100000) ctr_max

                                        ,percentile(like_rate*100000, 0.15)/100000 - 1.5* (percentile(like_rate*100000, 0.95)/100000-percentile(like_rate*100000, 0.15)/100000) like_rate_min
                                        ,percentile(like_rate*100000, 0.95)/100000 + 1.5* (percentile(like_rate*100000, 0.95)/100000-percentile(like_rate*100000, 0.15)/100000) like_rate_max

                                        ,percentile(share_rate*100000, 0.15)/100000 - 1.5* (percentile(share_rate*100000, 0.95)/100000-percentile(share_rate*100000, 0.15)/100000) share_rate_min
                                        ,percentile(share_rate*100000, 0.95)/100000 + 1.5* (percentile(share_rate*100000, 0.95)/100000-percentile(share_rate*100000, 0.15)/100000) share_rate_max

                                        ,percentile(download_rate*100000, 0.15)/100000 - 1.5* (percentile(download_rate*100000, 0.95)/100000-percentile(download_rate*100000, 0.15)/100000) download_rate_min
                                        ,percentile(download_rate*100000, 0.95)/100000 + 1.5* (percentile(download_rate*100000, 0.95)/100000-percentile(download_rate*100000, 0.15)/100000) download_rate_max

                                        ,percentile(dur*100000, 0.15)/100000 - 1.5* (percentile(dur*100000, 0.95)/100000-percentile(dur*100000, 0.15)/100000) dur_min
                                        ,percentile(dur*100000, 0.95)/100000 + 1.5* (percentile(dur*100000, 0.95)/100000-percentile(dur*100000, 0.15)/100000) dur_max

                                        ,percentile(detail_dur*100000, 0.15)/100000 - 1.5* (percentile(detail_dur*100000, 0.95)/100000-percentile(detail_dur*100000, 0.15)/100000) detail_dur_min
                                        ,percentile(detail_dur*100000, 0.95)/100000 + 1.5* (percentile(detail_dur*100000, 0.95)/100000-percentile(detail_dur*100000, 0.15)/100000) detail_dur_max

                                        ,percentile(play_rate*100000, 0.15)/100000 - 1.5* (percentile(play_rate*100000, 0.95)/100000-percentile(play_rate*100000, 0.15)/100000) as play_rate_min
                                        ,percentile(play_rate*100000, 0.95)/100000 + 1* (percentile(play_rate*100000, 0.95)/100000-percentile(play_rate*100000, 0.15)/100000) play_rate_max

                                        ,percentile(play_video*100000, 0.15)/100000 - 1.5* (percentile(play_video*100000, 0.95)/100000-percentile(play_video*100000, 0.15)/100000) play_video_min
                                        ,percentile(play_video*100000, 0.95)/100000 + 1.5* (percentile(play_video*100000, 0.95)/100000-percentile(play_video*100000, 0.15)/100000) play_video_max

                                        ,percentile(review_rate*100000, 0.15)/100000 - 1.5* (percentile(review_rate*100000, 0.95)/100000-percentile(review_rate*100000, 0.15)/100000) review_rate_min
                                        ,percentile(review_rate*100000, 0.95)/100000 + 1.5* (percentile(review_rate*100000, 0.95)/100000-percentile(review_rate*100000, 0.15)/100000) review_rate_max

                                        ,percentile(like_review_rate*100000, 0.15)/100000 - 1.5* (percentile(like_review_rate*100000, 0.95)/100000-percentile(like_review_rate*100000, 0.15)/100000) like_review_rate_min
                                        ,percentile(like_review_rate*100000, 0.95)/100000 + 1.5* (percentile(like_review_rate*100000, 0.95)/100000-percentile(like_review_rate*100000, 0.15)/100000) like_review_rate_max

                                        ,percentile(review_get_rate*100000, 0.15)/100000 - 1.5* (percentile(review_get_rate*100000, 0.95)/100000-percentile(review_get_rate*100000, 0.15)/100000) review_get_rate_min
                                        ,percentile(review_get_rate*100000, 0.95)/100000 + 1.5* (percentile(review_get_rate*100000, 0.95)/100000-percentile(review_get_rate*100000, 0.15)/100000) review_get_rate_max

                                        ,percentile(detail*100000, 0.15)/100000 - 1.5* (percentile(detail*100000, 0.95)/100000-percentile(detail*100000, 0.15)/100000) detail_min
                                        ,percentile(detail*100000, 0.95)/100000 + 1.5* (percentile(detail*100000, 0.95)/100000-percentile(detail*100000, 0.15)/100000) detail_max

                                        ,percentile(comment_dur*100000, 0.15)/100000 - 1.5* (percentile(comment_dur*100000, 0.95)/100000-percentile(comment_dur*100000, 0.15)/100000) comment_dur_min
                                        ,percentile(comment_dur*100000, 0.95)/100000 + 1.5* (percentile(comment_dur*100000, 0.95)/100000-percentile(comment_dur*100000, 0.15)/100000) comment_dur_max

                                        ,ptype
                                        ,part2_id
                                        ,group_age
                                        ,group_gender
                                    FROM
                                        ${post_data_table}
                                    WHERE ptype = 5
                                    group by ptype, part2_id, group_age, group_gender
                                )s
                        )stat
                        ON rg.ptype = stat.ptype and rg.group_gender = stat.group_gender and rg.group_age = stat.group_age and rg.part2_id = stat.part2_id
                        WHERE rg.ptype = 5
            ) A
      ) B
)C
"


sql_img="
DROP TABLE IF EXISTS ${img_table};
CREATE TABLE ${img_table} as
SELECT *, ROW_NUMBER() OVER(partition by group_gender, group_age, part2_id ORDER BY recall_score DESC) AS reviewed_rank
FROM (
      SELECT group_gender
            ,group_age
            ,part2_id
            ,pid
            ,round(case
            when group_gender='0' and group_age='1' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='0' and group_age='2' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='0' and group_age='3' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='0' and group_age='4' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='0' and group_age='0' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)

            when group_gender='1' and group_age='1' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='1' and group_age='2' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='1' and group_age='3' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='1' and group_age='4' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='1' and group_age='0' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)

            when group_gender='2' and group_age='1' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='2' and group_age='2' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='2' and group_age='3' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='2' and group_age='4' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            when group_gender='2' and group_age='0' then (sume_norm*0.34 + like_norm*0.2 + share_norm*0.2 + download_norm*0.2 + dur_norm*0.2 + detail_dur_norm*0.2 + review_rate_norm*0.4 + detail_norm*0.4)
            end, 4) as recall_score
            ,sume_norm
            ,like_norm
            ,share_norm
            ,download_norm
            ,dur_norm
            ,detail_dur_norm
            ,view_img_norm
            ,review_rate_norm
            ,like_review_rate_norm
            ,review_get_rate_norm
            ,detail_norm
            ,comment_dur_norm
      FROM (
            SELECT  pid
                    ,ptype
                    ,part2_id
                    ,group_age
                    ,group_gender
                    ,CASE WHEN sume_norm>1 THEN 1 WHEN sume_norm<0 THEN 0 ELSE sume_norm END sume_norm
                    ,CASE WHEN like_norm>1 THEN 1 WHEN like_norm<0 THEN 0 ELSE like_norm END like_norm
                    ,CASE WHEN share_norm>1 THEN 1 WHEN share_norm<0 THEN 0 ELSE share_norm END share_norm
                    ,CASE WHEN download_norm>1 THEN 1 WHEN download_norm<0 THEN 0 ELSE download_norm END download_norm
                    ,CASE WHEN dur_norm>1 THEN 1 WHEN dur_norm<0 THEN 0 ELSE dur_norm END dur_norm
                    ,CASE WHEN detail_dur_norm>1 THEN 1 WHEN detail_dur_norm<0 THEN 0 ELSE detail_dur_norm END detail_dur_norm
                    ,CASE WHEN view_img_norm>1 THEN 1 WHEN view_img_norm<0 THEN 0 ELSE view_img_norm END view_img_norm
                    ,CASE WHEN review_rate_norm>1 THEN 1 WHEN review_rate_norm<0 THEN 0 ELSE review_rate_norm END review_rate_norm
                    ,CASE WHEN like_review_rate_norm>1 THEN 1 WHEN like_review_rate_norm<0 THEN 0 ELSE like_review_rate_norm END like_review_rate_norm
                    ,CASE WHEN review_get_rate_norm>1 THEN 1 WHEN review_get_rate_norm<0 THEN 0 ELSE review_get_rate_norm END review_get_rate_norm
                    ,CASE WHEN detail_norm>1 THEN 1 WHEN detail_norm<0 THEN 0 ELSE detail_norm END detail_norm
                    ,CASE WHEN comment_dur_norm>1 THEN 1 WHEN comment_dur_norm<0 THEN 0 ELSE comment_dur_norm END comment_dur_norm
                    ,expose
            FROM    (
                        SELECT  rg.pid
                                ,rg.ptype
                                ,rg.part2_id
                                ,rg.group_age
                                ,rg.group_gender
                                ,COALESCE((ctr - ctr_min) / NULLIF((case when ctr_max>0 then ctr_max else 1 end - ctr_min),0), 0) sume_norm
                                ,COALESCE((like_rate - like_rate_min) / NULLIF((case when like_rate_max>0 then like_rate_max else 1 end - like_rate_min),0), 0) like_norm
                                ,COALESCE((share_rate - share_rate_min) / NULLIF((case when share_rate_max>0 then share_rate_max else 1 end - share_rate_min),0), 0) share_norm
                                ,COALESCE((download_rate - download_rate_min) / NULLIF((case when download_rate_max>0 then download_rate_max else 1 end - download_rate_min),0), 0) download_norm
                                ,COALESCE((dur - dur_min) / NULLIF((case when dur_max>0 then dur_max else 1 end - dur_min),0), 0) dur_norm
                                ,COALESCE((detail_dur - detail_dur_min) / NULLIF((case when detail_dur_max>0 then detail_dur_max else 1 end - detail_dur_min),0), 0) detail_dur_norm
                                ,COALESCE((view_img - view_img_min) / NULLIF((case when view_img_max>0 then view_img_max else 1 end - view_img_min),0), 0) view_img_norm
                                ,COALESCE((review_rate - review_rate_min) / NULLIF((case when review_rate_max>0 then review_rate_max else 1 end - review_rate_min),0), 0) review_rate_norm
                                ,COALESCE((like_review_rate - like_review_rate_min) / NULLIF((case when like_review_rate_max>0 then like_review_rate_max else 1 end - like_review_rate_min),0), 0) like_review_rate_norm
                                ,COALESCE((review_get_rate - review_get_rate_min) / NULLIF((case when review_get_rate_max>0 then review_get_rate_max else 1 end - review_get_rate_min),0), 0) review_get_rate_norm
                                ,COALESCE((detail - detail_min) / NULLIF((case when detail_max>0 then detail_max else 1 end - detail_min),0), 0) detail_norm
                                ,COALESCE((comment_dur - comment_dur_min) / NULLIF((case when comment_dur_max>0 then comment_dur_max else 1 end - comment_dur_min),0), 0) comment_dur_norm
                                ,rg.expose
                        FROM ${post_data_table} rg
                        LEFT OUTER JOIN (
                                SELECT
                                    ctr_max,like_rate_max,share_rate_max,download_rate_max,dur_max,detail_dur_max,view_img_max,review_rate_max,like_review_rate_max,review_get_rate_max,detail_max,comment_dur_max
                                    ,case when ctr_min>0 then ctr_min else 0 end as ctr_min
                                    ,case when like_rate_min>0 then like_rate_min else 0 end as like_rate_min
                                    ,case when share_rate_min>0 then share_rate_min else 0 end as share_rate_min
                                    ,case when download_rate_min>0 then download_rate_min else 0 end as download_rate_min
                                    ,case when dur_min>0 then dur_min else 0 end as dur_min
                                    ,case when detail_dur_min>0 then detail_dur_min else 0 end as detail_dur_min
                                    ,case when view_img_min>0 then view_img_min else 0 end as view_img_min
                                    ,case when review_rate_min>0 then review_rate_min else 0 end as review_rate_min
                                    ,case when like_review_rate_min>0 then like_review_rate_min else 0 end as like_review_rate_min
                                    ,case when review_get_rate_min>0 then review_get_rate_min else 0 end as review_get_rate_min
                                    ,case when detail_min>0 then detail_min else 0 end as detail_min
                                    ,case when comment_dur_min>0 then comment_dur_min else 0 end as comment_dur_min
                                    ,ptype
                                    ,part2_id
                                    ,group_age
                                    ,group_gender
                                from
                                (
                                    SELECT
                                        percentile(ctr*100000, 0.15)/100000 - 1.5* (percentile(ctr*100000, 0.95)/100000-percentile(ctr*100000, 0.15)/100000) ctr_min
                                        ,percentile(ctr*100000, 0.95)/100000 + 1.5* (percentile(ctr*100000, 0.95)/100000-percentile(ctr*100000, 0.15)/100000) ctr_max
                                        ,percentile(like_rate*100000, 0.15)/100000 - 1.5* (percentile(like_rate*100000, 0.95)/100000-percentile(like_rate*100000, 0.15)/100000) like_rate_min
                                        ,percentile(like_rate*100000, 0.95)/100000 + 1.5* (percentile(like_rate*100000, 0.95)/100000-percentile(like_rate*100000, 0.15)/100000) like_rate_max
                                        ,percentile(share_rate*100000, 0.15)/100000 - 1.5* (percentile(share_rate*100000, 0.95)/100000-percentile(share_rate*100000, 0.15)/100000) share_rate_min
                                        ,percentile(share_rate*100000, 0.95)/100000 + 1.5* (percentile(share_rate*100000, 0.95)/100000-percentile(share_rate*100000, 0.15)/100000) share_rate_max
                                        ,percentile(download_rate*100000, 0.15)/100000 - 1.5* (percentile(download_rate*100000, 0.95)/100000-percentile(download_rate*100000, 0.15)/100000) download_rate_min
                                        ,percentile(download_rate*100000, 0.95)/100000 + 1.5* (percentile(download_rate*100000, 0.95)/100000-percentile(download_rate*100000, 0.15)/100000) download_rate_max
                                        ,percentile(dur*100000, 0.15)/100000 - 1.5* (percentile(dur*100000, 0.95)/100000-percentile(dur*100000, 0.15)/100000) dur_min
                                        ,percentile(dur*100000, 0.95)/100000 + 1.5* (percentile(dur*100000, 0.95)/100000-percentile(dur*100000, 0.15)/100000) dur_max
                                        ,percentile(detail_dur*100000, 0.15)/100000 - 1.5* (percentile(detail_dur*100000, 0.95)/100000-percentile(detail_dur*100000, 0.15)/100000) detail_dur_min
                                        ,percentile(detail_dur*100000, 0.95)/100000 + 1.5* (percentile(detail_dur*100000, 0.95)/100000-percentile(detail_dur*100000, 0.15)/100000) detail_dur_max
                                        ,percentile(view_img*100000, 0.15)/100000 - 1.5* (percentile(view_img*100000, 0.95)/100000-percentile(view_img*100000, 0.15)/100000) view_img_min
                                        ,percentile(view_img*100000, 0.95)/100000 + 1.5* (percentile(view_img*100000, 0.95)/100000-percentile(view_img*100000, 0.15)/100000) view_img_max
                                        ,percentile(review_rate*100000, 0.15)/100000 - 1.5* (percentile(review_rate*100000, 0.95)/100000-percentile(review_rate*100000, 0.15)/100000) review_rate_min
                                        ,percentile(review_rate*100000, 0.95)/100000 + 1.5* (percentile(review_rate*100000, 0.95)/100000-percentile(review_rate*100000, 0.15)/100000) review_rate_max

                                        ,percentile(like_review_rate*100000, 0.15)/100000 - 1.5* (percentile(like_review_rate*100000, 0.95)/100000-percentile(like_review_rate*100000, 0.15)/100000) like_review_rate_min
                                        ,percentile(like_review_rate*100000, 0.95)/100000 + 1.5* (percentile(like_review_rate*100000, 0.95)/100000-percentile(like_review_rate*100000, 0.15)/100000) like_review_rate_max

                                        ,percentile(review_get_rate*100000, 0.15)/100000 - 1.5* (percentile(review_get_rate*100000, 0.95)/100000-percentile(review_get_rate*100000, 0.15)/100000) review_get_rate_min
                                        ,percentile(review_get_rate*100000, 0.95)/100000 + 1.5* (percentile(review_get_rate*100000, 0.95)/100000-percentile(review_get_rate*100000, 0.15)/100000) review_get_rate_max

                                        ,percentile(detail*100000, 0.15)/100000 - 1.5* (percentile(detail*100000, 0.95)/100000-percentile(detail*100000, 0.15)/100000) detail_min
                                        ,percentile(detail*100000, 0.95)/100000 + 1.5* (percentile(detail*100000, 0.95)/100000-percentile(detail*100000, 0.15)/100000) detail_max

                                        ,percentile(comment_dur*100000, 0.15)/100000 - 1.5* (percentile(comment_dur*100000, 0.95)/100000-percentile(comment_dur*100000, 0.15)/100000) comment_dur_min
                                        ,percentile(comment_dur*100000, 0.95)/100000 + 1.5* (percentile(comment_dur*100000, 0.95)/100000-percentile(comment_dur*100000, 0.15)/100000) comment_dur_max

                                        ,ptype
                                        ,part2_id
                                        ,group_age
                                        ,group_gender
                                    FROM
                                        ${post_data_table}
                                    WHERE ptype <> 5
                                    group by ptype, part2_id, group_age, group_gender
                                )s
                        )stat
                        ON rg.ptype = stat.ptype and rg.group_gender = stat.group_gender and rg.group_age = stat.group_age and rg.part2_id = stat.part2_id
                        WHERE rg.ptype <> 5
            ) A
      ) B
)C
"


echo "start gen ${post_data_table}"
${odpscmd} -e "${sql}"


echo "start gen ${video_table}"
${odpscmd} -e "${sql_video}"

echo "start gen ${img_table}"
${odpscmd} -e "${sql_img}"


${odpscmd} -e "tunnel download ${video_table} ./olduser_video_hot_part2_age_gender.csv"
${odpscmd} -e "tunnel download ${img_table} ./olduser_img_hot_part2_age_gender.csv"
echo "end download"


echo "upload into redis"
${pythonenv} upload_ordered_pids_into_redis_group.py './olduser_video_hot_part2_age_gender.csv' 'index_olduser_video_hot_part2_age_gender_' 2 1 0 --pid_index 3 --top 50000 --score_index 4
${pythonenv} upload_ordered_pids_into_redis_group.py './olduser_img_hot_part2_age_gender.csv' 'index_olduser_img_hot_part2_age_gender_' 2 1 0 --pid_index 3 --top 50000 --score_index 4


echo "done"
