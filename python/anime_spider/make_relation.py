import requests
import json
import re
import pandas as pd
import time
from multiprocessing.dummy import Pool as ThreadPool
import csv
import traceback


# 1.多线程运作时，务必不要将写入操作和爬虫请求写在一起，死锁可能造成error，然后全部GG
#
# 2.四核电脑(相对于本机而言)线程数最好不要超过32，不然容易死锁效率也无法提升，有的时候根本没必要多线程的时候就别用，先自己测试一下效率再考虑是否用多线程
#
# 3.先将爬虫数据保存csv，然后通过csv再建立关系，原因同上
#
# 4.ThreadPool务必注意将map的key函数改成传参模式，将写入操作把列表加上全局变量声明，写在key的函数内
#
# 5.前端数据注意先测试几组数据再总体爬虫，防止前端内容不一致的错误导致正则出错，比如'：'和':'和多余的"\\t" "\n"等等
#
# 6.相对于bilibili而言，要注意爬取的是json中的media_id而不是播放页面的play参数，两者差异巨大容易混淆
#
# 7.相对于bilibili而言，反爬虫机制弱到几乎没有(但用一)，但还是要注意异常处理和请求超时、请求间隔
#
# 8.Http_header可有可无(限bilibili，但长期请求过多频率过快也会被封IP)，看心情
#
# 9.neo4j的中文默认编码为utf8无bom模式,程序内无法解决编码问题就去文档修改编码
#
# 10.python编码出现问题必要时候可以转换成raw_unicode_escape编码再进行encode
#
# 11.关系和节点最好不要一起建，因为节点如果属性只有一个的话可能会重复，MD



#csv总体保存
def getVoiceActor(num):

    url = "https://www.bilibili.com/bangumi/media/md" + str(num).rstrip("\n")
    html = requests.get(url)
    con = html.content.decode("utf8")

    if con.count("出错啦") != 0:
        print('error')
    else:
        pat_actors = '"actors":".+?"'
        res = re.findall(pat_actors, con)
        actors_in_one_anime = []
        if len(res) != 0 and ",\"" not in res[0]:
            actors = res[0].lstrip("\"actors\":").split("\\n")
            if "：" in actors[0]:
                actors = [i.split("：") for  i in actors if "：" in i ]
                for i in actors:
                    actors_in_one_anime.append(i[0])
                    actors_in_one_anime.append(i[1])

            elif ":" in actors[0]:
                actors = [i.split(":") for  i in actors if ":" in i ]
                for i in actors:
                    actors_in_one_anime.append(i[0])
                    actors_in_one_anime.append(i[1])

        #有些番剧在声优后面会加上\\t，去除
        for i in range(0,len(actors_in_one_anime)):
            actors_in_one_anime[i] = actors_in_one_anime[i].rstrip("\\t")



        #列表最后一个元素可能会出现xxxx "、xxxx"、xxxx.."、.."  等多余字符(多余人员消息)           全て殺す！
        actors_in_one_anime[len(actors_in_one_anime) - 1] = actors_in_one_anime[len(actors_in_one_anime)-1].rstrip('"')

        if  len(actors_in_one_anime) != 0:
            if ".." in actors_in_one_anime[len(actors_in_one_anime)-1]:
                actors_in_one_anime.pop()
                actors_in_one_anime.pop()

        #把声优列表加上对应的media_id
        actors_in_one_anime.insert(0,num.rstrip("\n"))

        #去除没有声优的番剧 FAQ
        global num_and_voice_actors
        if len(actors_in_one_anime) != 1:
            num_and_voice_actors.append(actors_in_one_anime)


# num = media_id -> anime_index_id
# actors_in_one_anime[1.3.5...]

#line[0] is media_id , others is voice_actors

def make_nodes(line):
    global sum_count
    global character_index_count, character_index_id, character_name
    global voice_actor_index_count, voice_actor_index_id, voice_actor_name
    global character_and_voice_start_id, character_and_voice_end_id
    global anime_and_voice_actor_start_id, anime_and_voice_actor_end_id
    global anime_and_character_start_id, anime_and_character_end_id

    #character ensure

    for i in range(1,len(line),2):
        if line[i] not in character_name:
            character_index_count += 1
            character_index_id.append(character_index_count)
            character_name.append(line[i])

    #voice_actor ensure
    for i in range(2,len(line),2):
        if line[i] not in voice_actor_name:
            voice_actor_index_count += 1
            voice_actor_index_id.append(voice_actor_index_count)
            voice_actor_name.append(line[i])

def get_voice_actor_index_id(name):
    voice_actors = csv.reader(open("d://voice_actors.csv", 'r', encoding="utf8"), dialect="excel")
    voice_actors = [i for i in voice_actors]
    for i in voice_actors:
        if i[1] == name:
            return i[0]

def get_character_index_id(name):
    characters = csv.reader(open("d://characters.csv", 'r', encoding="utf8"), dialect="excel")
    characters = [i for i in characters]
    for i in characters:
        if i[1] == name:
            return i[0]

def make_releations(line):
    line = [i.lstrip(" ").rstrip(" ") for i in line]
    global sum_count
    global character_index_count,character_index_id,character_name
    global voice_actor_index_count,voice_actor_index_id,voice_actor_name
    global character_and_voice_start_id,character_and_voice_end_id
    global anime_and_voice_actor_start_id,anime_and_voice_actor_end_id
    global anime_and_character_start_id,anime_and_character_end_id

    #anime_id ensure
    animes = csv.reader(open("d://animes.csv", 'r', encoding="utf8"), dialect="excel")
    animes= [i for i in animes]
    media_index = ""
    for i in animes:
        if str(i[1]).rstrip("\n") == str(line[0]).rstrip("\n"):
            media_index = i[0]

    # anime_and_actor  anime_and_chararter ensure
    if media_index != "":
        for i in range(1,len(line)):
            #character
            if i%2 == 1:
                a = get_character_index_id(line[i])
                if a == None:
                    return
                anime_and_character_start_id.append(media_index)
                anime_and_character_end_id.append(a)
                character_and_voice_start_id.append(a)

            # actor
            else:
                b = get_voice_actor_index_id(line[i])
                if b == None:
                    anime_and_character_start_id.pop()
                    anime_and_character_end_id.pop()
                    character_and_voice_start_id.pop()
                    return
                anime_and_voice_actor_start_id.append(media_index)
                anime_and_voice_actor_end_id.append(b)
                character_and_voice_end_id.append(b)


        sum_count += 1
        print(sum_count)


if __name__ == '__main__':

    sum_count = 0

    f = open("d://bangumiNum.txt", 'r')
    bangumiNum = f.readlines()

    character_index_count = 300000
    character_index_id = []
    character_label = []
    character_name = []

    voice_actor_index_count = 400000
    voice_actor_index_id = []
    voice_actor_label = []
    voice_actor_name = []

    character_and_voice_start_id = []
    character_and_voice_end_id = []
    character_and_voice_releation = []

    anime_and_voice_actor_start_id = []
    anime_and_voice_actor_end_id = []
    anime_and_voice_actor_releation = []

    anime_and_character_start_id = []
    anime_and_character_end_id = []
    anime_and_character_releation = []


    ##media_id和actors关系保存至csv
    # num_and_voice_actors = []
    #
    # pool = ThreadPool(64)
    # pool.map(getVoiceActor,bangumiNum)
    # pool.close()
    # pool.join()
    #
    # print("Starting writing")
    # c = csv.writer(open("d://media_id_and_voice_actors.csv",'w',newline = "",encoding="utf8"),dialect="excel")
    # for i in num_and_voice_actors:
    #     c.writerow(i)

    id_and_voice = csv.reader(open("d://media_id_and_voice_actors.csv", 'r', encoding="utf8"), dialect="excel")

    id_and_voice = [i for i in id_and_voice]

    # pool = ThreadPool(8)
    # pool.map(make_releations,id_and_voice)
    # pool.close()
    # pool.join()


    for i in id_and_voice:
        # make_nodes(i)
        make_releations(i)

    # print(get_character_index_id("因幡洋"))
    # make_releations(id_and_voice[0])

    # for i in id_and_voice:
    #     if i[0] == "1436":
    #         make_releations(i)

    # print(get_character_index_id(""))
    print("-----------------------------------")
    # print(get_voice_actor_index_id("松冈祯丞"))
    # print(get_character_index_id("菲莉尔·克雷斯特"))
    print("starting writing")

    # ##############################################################
    # for i in character_name:
    #     character_label.append("character")
    # df = pd.DataFrame({"index:ID": character_index_id,
    #                    "name":character_name,
    #                    ":LABEL": character_label})
    # df.to_csv("d://characters.csv",index = False)
    #
    #
    #
    # ##############################################################
    # for i in voice_actor_name:
    #     voice_actor_label.append("voice_actor")
    # df = pd.DataFrame({"index:ID": voice_actor_index_id,
    #                    "name":voice_actor_name,
    #                    ":LABEL": voice_actor_label})
    # df.to_csv("d://voice_actors.csv",index = False)


    ##############################################################
    for i in anime_and_voice_actor_start_id:
        anime_and_voice_actor_releation.append("出演声优")
    anime_and_voice_actor_type = anime_and_voice_actor_releation

    for i in anime_and_character_start_id:
        anime_and_character_releation.append("拥有角色")
    anime_and_character_type = anime_and_character_releation

    for i in character_and_voice_start_id:
        character_and_voice_releation.append("所配角色")
    character_and_voice_type = character_and_voice_releation

    df = pd.DataFrame({":START_ID": anime_and_voice_actor_start_id,
                       ":END_ID": anime_and_voice_actor_end_id,
                       "relation": anime_and_voice_actor_releation,
                       ":TYPE":anime_and_voice_actor_type})
    df.to_csv("d://anime_and_voice_actor_releation.csv", index=False)

    df = pd.DataFrame({":START_ID": anime_and_character_start_id,
                       ":END_ID": anime_and_character_end_id,
                       "relation": anime_and_character_releation,
                       ":TYPE": anime_and_character_type})
    df.to_csv("d://anime_and_character_releation.csv", index=False)

    df = pd.DataFrame({":START_ID": character_and_voice_start_id,
                       ":END_ID": character_and_voice_end_id,
                       "relation": character_and_voice_releation,
                       ":TYPE": character_and_voice_type})
    df.to_csv("d://character_and_voice_releation.csv", index=False)










