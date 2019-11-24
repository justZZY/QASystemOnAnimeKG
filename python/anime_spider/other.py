import requests
import json
import re
import pandas as pd
import time
from multiprocessing.dummy import Pool as ThreadPool
import csv

header = {
    'Connection':'keep-alive',
    'Cookie':'fts=1536416085; CURRENT_FNVAL=8; buvid3=50E8C273-167F-49E6-90B4-0277D6B74C836684infoc; rpdid=omqsxlqlskdoskxpoxwpw; LIVE_BUVID=AUTO8315364171339118; UM_distinctid=165b9a7d7ee42d-07910e2580ad6e-5701631-144000-165b9a7d7ef6f6; sid=bp56qxap; CURRENT_QUALITY=80; im_notify_type_4706940=0; stardustvideo=0; finger=edc6ecda; _uuid=9D9ED67F-BC4C-AB18-1DAF-49770A6FA40087442infoc; bp_t_offset_4706940=173596612395821802; DedeUserID=4706940; DedeUserID__ckMd5=497ed1bad272fc35; SESSDATA=521e6037%2C1541862177%2C63126944; bili_jct=cb22cd15771d411753d1f9b1094beb26; _dfcaptcha=2d25ec1f4e70ab7fd278b5f8aa896cda; BANGUMI_SS_25613_REC=250674',
    'Host':'www.bilibili.com',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
}

#修正连载不显示而为空格的问题
def fix():
    x = csv.reader(open("d://animes.csv"))
    x = [i for i in x]

    out = open("d://animess.csv",'a',newline="")
    csv_write = csv.writer(out,dialect='excel')

    for i in range(0,len(x)):
        # for k in range(0,len(x[i])):
        #     x[i][k] = x[i][k].encode("raw_unicode_escape")
        if len(x[i][9]) is 0:
            x[i][9] = "连载中"

    for i in x:
        csv_write.writerow(i)

def getBangumiNum():
    bangumi_num = []
    for page in range(1,155):
        html = requests.get("https://bangumi.bilibili.com/media/web_api/search/result?season_version=-1&area=-1&is_finish=-1&copyright=-1&season_status=-1&season_month=-1&pub_date=-1&style_id=-1&order=3&st=1&sort=0&page="+str(page)+"&season_type=1&pagesize=20")
        hjson = json.loads(html.content.decode("utf8"))
        bangumi = hjson['result']['data']

        for i in bangumi:
            if i['media_id'] not in bangumi_num:
                bangumi_num.append(i['media_id'])
        print("The page : " + str(page))

    f = open("d://bangumiNum.txt",'a')
    for i in bangumi_num:
        f.write(str(i)+'\n')
    f.close()
    print("Bangumi Number load ok")


#注意不是每个番剧都有详情页面
def getDetail(num):

    url = "https://www.bilibili.com/bangumi/media/md"+str(num).rstrip("\n")
    html = requests.get(url)
    con = html.content.decode("utf8")

    if con.count("出错啦") != 0:
            print('error')
    else:
        # #staff为制作人员表
        # pat = [r"[\\n]?(监督|导演)：.+?\\n",r"人物设定：.+?\\n",r"原作：.+?\\n",r"系列构成：.+?\\n",r"色彩设计：.+?\\n",r"人物原案：.+?\\n",r"美术监督：.+?\\n",r"音乐：.+?\\n",r"动画制作：.+?\s"]
        # staff = []
        #
        # if len(re.findall(pat[0],con))!=0:
        #     staff.append(re.findall(pat[0],con)[0].rstrip("\\n").split("："))        #staff大概格式如下
        #     #[['导演', '浅井義之'], ['人物设定', '山田有慶'], ['原作', '東出祐一郎、TYPE-MOON'], ['系列构成', '東出祐一郎'], ['色彩设计', '土居真紀子']]
        #     return staff[0][0]
        #
        # else:
        #     return ""

        #title为番剧名称
        pat_actors = '<span class="media-info-title-t">.+?</span>'
        titles = re.findall(pat_actors,con)
        title = ""
        for i in titles:
            title = i.lstrip('<span class="media-info-title-t">').rstrip("</span>")


        # pat_actors = '"actors":".+?"'
        # res = re.findall(pat_actors, con)
        # if len(res) != 0 and ",\"" not in res[0]:
        #     actors = res[0].lstrip("\"actors\":").split("\\n")
        #     if "：" in actors[0]:
        #         actors = [i.split("：") for  i in actors if "：" in i ]
        #
        #     elif ":" in actors[0]:
        #         actors = [i.split(":") for  i in actors if ":" in i ]



        # actors大概格式如下
        # [['齐格', '花江夏樹'], ['贞德', '坂本真綾'], ['狮子劫界离', '乃村健次'], ['莫德雷德', '沢城みゆき'], ['天草四郎', '内山昂輝']]

        # tag为番剧类型
        # pat_mediaTag = '<span class="media-tag">.+?</span>'
        # mediaTag = re.findall(pat_mediaTag,con)
        # mediaTag = [i.lstrip('<span class="media-tag">').rstrip("</span>") for i in mediaTag]
        # global mediaTag_id
        # global mediaTags
        # global count
        #
        # for i in mediaTag:
        #     if i not in mediaTags:
        #         count += 1
        #         mediaTag_id.append(count)
        #         mediaTags.append(i)
        #         print(str(count)+"   "+str(i))



        # tag大概格式如下
        # ['热血', '战斗']

        #times为播放量   time is string
        pat_times = "总播放</span> <em>.+?</em>"
        if re.findall(pat_times,con):
            times = re.findall(pat_times,con)[0].lstrip("总播放</span> <em>").rstrip("</em>'")
            file_anime_media_attr_1.append(times)
        else:
            file_anime_media_attr_1.append("0")


        #people为追番人数    people is string
        pat_people = "追番人数</span> <em>.+?</em>"
        ok = re.findall(pat_people, con)
        if ok:
            people = re.findall(pat_people, con)[0].lstrip("追番人数</span> <em>").rstrip("</em>'")
            file_anime_media_attr_2.append(people)
        else:
            file_anime_media_attr_2.append("0")


        #danmucount为弹幕总数 danmu is string
        pat_danmu = "弹幕总数</span> <em>.+?</em>"
        danmu = re.findall(pat_danmu, con)[0].lstrip("弹幕总数</span> <em>").rstrip("</em>'")
        file_anime_media_attr_3.append(danmu)

        #starting time
        pad_starting_time = "<span>.+?开播"
        starting_time = re.findall(pad_starting_time,con)[0].lstrip("<span>").rstrip("开播")
        file_anime_media_attr_4.append(starting_time)

        #score
        pat_score = '<div class="media-info-score-content"><div>.+?</div>'
        ok = re.findall(pat_score, con)
        if ok:
            score = re.findall(pat_score, con)[0].lstrip('<div class="media-info-score-content"><div>').rstrip("</div>")
            file_anime_media_attr_5.append(score)
        else:
            file_anime_media_attr_5.append("0")

        #cal_num_of_people
        pat_score_people = ' <div class="media-info-review-times">.+?</div>'
        ok = re.findall(pat_score_people, con)
        if ok:
            score_people = re.findall(pat_score_people, con)[0].lstrip('<div class="media-info-review-times"><div>').rstrip("</div>")
            file_anime_media_attr_6.append(score_people)
        else:
            file_anime_media_attr_6.append("0")


        #status
        status = ""
        if con.count("已完结"):
            status = "已完结"
        else:
            stauts = "连载中"

        file_anime_media_attr_7.append(status)

        global file_anime_index
        global file_anime_name
        global count
        global file_anime_media_id

        file_anime_name.append(title)
        count += 1
        file_anime_index.append(count)
        file_anime_media_id.append(num)

        print(str(count) + "   " + str(num).rstrip("\n") + "   " + title)
