import requests
import json
import re
import pandas as pd
import time
from multiprocessing.dummy import Pool as ThreadPool
import csv
import traceback

def getType(num):
    url = "https://www.bilibili.com/bangumi/media/md" + str(num).rstrip("\n")
    html = requests.get(url)
    con = html.content.decode("utf8")

    pat_mediaTag = '<span class="media-tag">.+?</span>'
    mediaTag = re.findall(pat_mediaTag,con)
    # if len(mediaTag) != 0:
    mediaTag = [i.lstrip('<span class="media-tag">').rstrip("</span>") for i in mediaTag]

    # print(mediaTag)
    global catas

    global count_spider

    for i in mediaTag:
        catas.append(mediaTag[0])
        count_spider += 1
        print(count_spider)
        f = open("d://r.txt", 'a',encoding="utf8")
        f.write(str(num).rstrip("\n") + " " +str(i).rstrip("\n")+"\n")
        f.close()


def make_releation_between_catagory_and_anime(num_cata):
    global file_animes, file_cata, count

    num_cata = num_cata.split(" ")
    count += 1
    print(str(count) + "  " + num_cata[0] + " " + num_cata[1])
    num_cata[1] = num_cata[1].rstrip("\n")

    #search anime_index_id(starting_id) by num
    #Then search type_id (end_id) by getType()
    tmp_a = ""
    for a in file_animes:
        if int(a[1]) == int(num_cata[0]):
            start_id.append(a[0])
            tmp_a=a[1]
            # print(str(count)+"  start ok: "+ str(a[1]),end="")
            break

    tmp_ok = 0
    #多个类型
    for t in file_cata:
        if str(num_cata[1]) == str(t[1]).rstrip("\n") and tmp_ok == 0:
            end_id.append(t[0])
            # print("  end ok : " + str(t[0]),end="")
            tmp_ok = 1

        elif str(num_cata[1]) == str(t[1]).rstrip("\n") and tmp_ok == 1:
            start_id.append(tmp_a)
            end_id.append(t[0])






if __name__ == '__main__':


    f = open("d://bangumiNum.txt", 'r')
    bangumiNum = f.readlines()

    start_id = []
    end_id = []

    file_cata = csv.reader(open("d://catagorys.csv", 'r',encoding="utf-8"), dialect="excel")
    file_cata = [i for i in file_cata]
    file_cata = file_cata[1:]
    #[['index:ID', 'name', ':LABEL'], ['20001', '奇幻', '类型'], ['20002', '校园', '类型'].....]

    file_animes = csv.reader(open("d://animes.csv", 'r'), dialect="excel")
    file_animes = [i for i in file_animes]
    file_animes = file_animes[1:]

    count = 0


    file_r = open("d://r.txt", 'r', encoding="utf8")

    num_cata = file_r.readlines()

    pool = ThreadPool(32)
    pool.map(make_releation_between_catagory_and_anime,num_cata)
    pool.close()
    pool.join()
    # make_releation_between_catagory_and_anime(num_cata[0])

    relation = ["类型属于" for i in range(0, len(start_id))]

    df = pd.DataFrame({":START_ID": start_id, ":END_ID": end_id, "relation": relation, ":TYPE": relation})

    df.to_csv("d://anime_type.csv",index = False)



    catas = []
    count_spider = 0
    pool = ThreadPool(32)
    pool.map(getType, bangumiNum)
    pool.close()
    pool.join()
    
    for i in bangumiNum:
        getType(i)

    count = 20000
    mediaTag_id = []
    mediaTags = []
    label = '类型'
    
    pool = ThreadPool(32)
    pool.map(getDetail,bangumiNum)
    pool.close()
    pool.join()
    
    df = pd.DataFrame({"index:ID": mediaTag_id,
                       "name":mediaTags,
                       ":LABEL": "类型"})
    df.to_csv("d://catagorys.csv",index = False)


    count = 10000
    file_anime_index = []
    file_anime_name = []
    file_anime_media_id = []
    file_anime_media_attr_1 = []
    file_anime_media_attr_2 = []
    file_anime_media_attr_3 = []
    file_anime_media_attr_4 = []
    file_anime_media_attr_5 = []
    file_anime_media_attr_6 = []
    file_anime_media_attr_7 = []
    file_anime_label = "Anime"
    
    pool = ThreadPool(64)
    pool.map(getDetail,bangumiNum)
    pool.close()
    pool.join()
    
    df = pd.DataFrame({"index:ID": file_anime_index,
                       "动漫序列号":file_anime_media_id,
                       "动漫名称": file_anime_name,
                       "播放数量": file_anime_media_attr_1,
                       "追番人数": file_anime_media_attr_2,
                       "弹幕总数": file_anime_media_attr_3,
                       "开播时间": file_anime_media_attr_4,
                       "点评分数": file_anime_media_attr_5,
                       "评分人数": file_anime_media_attr_6,
                       "状态": file_anime_media_attr_7,
                       ":LABEL": file_anime_label})
    df.to_csv("d://animes.csv",index = False)



