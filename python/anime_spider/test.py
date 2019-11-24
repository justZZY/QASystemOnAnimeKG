# from multiprocessing.dummy import Pool as ThreadPool
#
# file_r = open("d://r.txt", 'r', encoding="utf8")
#
# num_cata = file_r.readlines()
#
#
#
# num_cata = [i for i in num_cata if len(i.split(" ")) is 2 ]
#
# num_cata = list(set(num_cata))
#
# file_rr = open("d://rr.txt", 'a', encoding="utf8")
#
# for i in num_cata:
#     file_rr.writelines(i)







#  remove the "\n
# import csv
#
#
# x = csv.reader(open("d://animes.csv",'r'),dialect="excel")
#
# x = [i for i in x]
#
# for i in range(1,len(x)):
#     x[i][1] = x[i][1].rstrip("\n")
#
#
# c = csv.writer(open("d://animess.csv",'w',newline = ""),dialect="excel")
# for i in x:
#     c.writerow(i)





#media_id and actors的csv是否每行为奇数

import csv
x = csv.reader(open("d://character_and_voice_releation.csv",'r',encoding="utf8"),dialect="excel")

x = [i for i in x]


print(len(x))
new = []
for i in x:
    if i not in new:
        new.append(i)

print(len(new))

x = new
c = csv.writer(open("d://character_and_voice_releation_2.csv",'w',newline = "",encoding="utf8"),dialect="excel")
for i in x:
    c.writerow(i)




