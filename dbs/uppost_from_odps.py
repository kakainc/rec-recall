import requests
import json
from retry import retry
from datetime import date, timedelta
import odps_cmd
from requests.exceptions import RequestException


def get_pids():
    start_day = (date.today() + timedelta(days=-5)).strftime("%Y%m%d")
    odps = odps_cmd.get_odpscmd()
    sql = '''select pid, sum(review) as review_cnt, count(*) as expose, sum(review)/count(*) as comment_rate 
                from(
                select a.pid, if(create_review+reply_review>=2, 2, create_review+reply_review) as review
                from omg_user_post_score a
                left outer join omg_postmetadata b
                on a.pid=b.pid 
                where to_char(from_unixtime(b.ct), 'yyyymmdd')>="{0}" and a.ymd>="{0}" and a.expose>0
                )
                group by pid
                having review_cnt>=5 and comment_rate>=0.004;'''.format(start_day)
    res = {}
    with odps.execute_sql(sql).open_reader() as reader:
        for record in reader:
            pid = int(record.pid)
            reviews = str(record.review_cnt)
            expose = str(record.expose)
            comment_rate = str(record.comment_rate)
            res[pid] = [reviews, expose, comment_rate]
    return res


@retry(Exception, tries=3, delay=1)
def upload_exposure(pid, reviews, expose, comment_rate):
    url = 'http://xxxx.net/communityhighaudit'
    response = requests.post(url, data=json.dumps(
        {"pid": pid, "ext": {"reviews": reviews, "expose": expose, "comment_rate": comment_rate}}))
    if response.status_code != 200:
        raise RequestException('status_code:{0}, pid:{1}'.format(response.status_code, pid))
    content = json.loads(response.content)
    ret = content.get('ret', 0)
    if ret != 1:
        msg = content.get('msg', '')
        # print 'ret:{0}, pid:{1}, msg:{2}'.format(ret, pid, msg)
    return True


if __name__ == '__main__':
    res = get_pids()
    print("begin request")
    for pid, info in res.items():
        try:
            upload_exposure(pid, info[0], info[1], info[2])
        except RequestException as e:
            print(str(e))
    print("end request")
