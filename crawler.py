import requests
import re
from bs4 import BeautifulSoup
import time
import sys
import os
from peewee import *

db_file_path = os.path.expanduser('~/Projects/1point3acres-crawler/1p3a.sqlite')
db = SqliteDatabase(db_file_path)


class Thread(Model):
    thread_id = IntegerField()
    title = TextField(null=True)
    url = TextField()

    season = TextField(null=True)
    job_category = TextField(null=True)
    edu_type = TextField(null=True)
    work_type = TextField(null=True)
    company = TextField(null=True)
    app_type = TextField(null=True)
    interview_type = TextField(null=True)
    result = TextField(null=True)
    applicant_type = TextField(null=True)
    post_date = DateTimeField(null=True)

    class Meta:
        database = db


db.connect()
db.create_tables([Thread])


# 第一个 column 的主要信息都在 <th class="common"></th> 中, 若该 thread 未读, 则 class name 为 "new"
def parse_thread(tag_common, tag_post_by, check_all_threads=False):
    a_title = tag_common.find('a', {'class': 's xst'})
    title = a_title.text
    thread_id = re.search('tid=(\d+)', a_title['href'])[1]

    if Thread.select().where(Thread.thread_id == thread_id).exists():
        if not check_all_threads:
            print('Update complete')
            exit(1)
        else:
            return

    url = 'http://www.1point3acres.com/bbs/thread-{}-1-1.html'.format(thread_id)
    print(title, url)

    tag_main_info = tag_common.find('span', {'style': 'margin-top: 3px'})

    thread = Thread()
    thread.thread_id = thread_id
    thread.title = title
    thread.url = url

    if tag_main_info is not None:
        tag_job = tag_main_info.contents[3].contents

        job_category = tag_job[0].text
        edu_type = tag_job[2].text
        work_type = tag_job[4].text
        company = tag_job[6].text

        season = tag_main_info.contents[1].text
        app_type = str(tag_main_info.contents[4]).replace('-', '').replace(' ', '')
        interview_type = tag_main_info.contents[5].text
        result = tag_main_info.contents[7].text
        applicant_type = str(tag_main_info.contents[8]).replace(' ', '').replace('|', '').replace('\n', '')

        thread.season = season
        thread.job_category = job_category
        thread.edu_type = edu_type
        thread.work_type = work_type
        thread.company = company
        thread.interview_type = interview_type
        thread.app_type = app_type
        thread.result = result
        thread.applicant_type = applicant_type

        print(season, job_category, edu_type, work_type, company, app_type, interview_type, result, applicant_type)

    author = tag_post_by.find('a').text if tag_post_by.find('a') else '论坛匿名账号'

    # 1天内有两个span, 第一个span有个class='ni1'
    # 1-7天内有两个span, 无 class='ni1'
    # 7天后直接就是 <span>2018-10-12</span>, 里面没任何其他东西
    post_date = tag_post_by.find('span')
    post_date = post_date.find('span')['title'] if post_date.find('span') else post_date.text
    thread.post_date = post_date

    print(author, post_date)
    print()
    print()

    thread.save()


# 有些 info 全为空的 thread 只有 GET 才会显示, POST 不会返回这些结果
# 如 GET: https://www.1point3acres.com/bbs/forum.php?mod=forumdisplay&fid=145
# 和 直接点搜索按钮返回的结果数量差了几百条, 差的应该就是那些 thread info 为空的帖子
#
# 所以我先通过 GET 拿到最近的1000个帖子 (GET 只返回最近1000条), 再通过 POST 爬所有年份的帖子
def crawl_all_data():
    crawl_latest(check_all_threads=True)

    start_year, end_year = 2011, 2029
    offset = 2010
    for year in range(start_year-offset, end_year-offset+1, 1):
        crawl(check_all_threads=True, year=year)


def crawl(check_all_threads=False, get_request=False, year=0):
    page, pages = 1, 99999
    while page <= int(pages):
        time.sleep(1)  # 降低请求频率
        url = 'https://www.1point3acres.com/bbs/forum.php'

        # url query string
        params = {
            'mod': 'forumdisplay',
            'fid': '145',
            'sortid': '311',
            'orderby': 'dateline',
            'page': page,
        }

        # POST form data: 找工年度
        if not get_request:  # POST
            data = {
                'searchoption[3086][value]': str(year),
                'searchoption[3086][type]': 'radio',
                'searchoption[3087][value]': '0',
                'searchoption[3087][type]': 'radio',
                'searchoption[3088][value]': '0',
                'searchoption[3088][type]': 'radio',
                'searchoption[3089][type]': 'checkbox',
                'searchoption[3090][value]': '0',
                'searchoption[3090][type]': 'radio',
                'searchoption[3048][value]': '0',
                'searchoption[3048][type]': 'radio',
                'searchoption[3092][value]': '0',
                'searchoption[3092][type]': 'radio',
                'searchoption[3046][value]': '0',
                'searchoption[3046][type]': 'radio',
                'searchoption[3091][value]': '0',
                'searchoption[3091][type]': 'radio',
                'searchoption[3109][value]': '0',
                'searchoption[3109][type]': 'radio',
            }
            html = requests.post(url, params=params, data=data).text
        else:
            html = requests.post(url, params=params).text

        soup = BeautifulSoup(html, "html.parser")

        # 更新总页数
        if pages == 99999:
            if soup.find(id='fd_page_bottom').find('span'):
                pages = re.search('.*?(\d+).*', soup.find(id='fd_page_bottom').find('span')['title'])[1]
            else:
                pages = 1
            print(pages, 'pages in total\n')

        thread_tags = soup.findAll('tbody', id=re.compile("^normalthread_*"))

        for thread_tag in thread_tags:
            tag_common = thread_tag.find('th')
            tag_post_by = thread_tag.find('td', {'class': 'by'})
            parse_thread(tag_common, tag_post_by, check_all_threads)

        print('Finished crawling page {} of {} with {} '.format(page, pages, 'GET' if get_request else 'POST'))
        sys.stdout.flush()
        if not get_request:
            print('in year', year+2010)
        page += 1


def crawl_latest(check_all_threads=False):
    crawl(check_all_threads, get_request=True)


if __name__ == '__main__':
    # crawl_all_data()  # crawl all 面经 from scratch
    crawl_latest()
