from wordcloud import WordCloud
# python的可视化库，也是二级考试推荐的可视化库
import matplotlib.pyplot as plt
import jieba
from mongodb import keyword_keyword_oper
import re
import jieba
import paddle

import logging


paddle.enable_static()
jieba.enable_paddle()

jieba.setLogLevel(logging.WARNING)

s = "今天下雨了123！@#%@……￥@￥，不开心"
#去除不可见字符
def c(s):
    return re.sub('[^\u4e00-\u9fa5]+','',s)
print(str)

word_list=[]
text=keyword_keyword_oper.find_all()
#读出所有推文文本
for t in text:
    print(t)
    words = jieba.lcut(c(t),use_paddle=True)
    word_list+=words
# str='杭州亚运会啊亚运会'
# words = jieba.lcut(str)
#
# print(words)
# #join 函数 用斜杆拼接词组mask =maskph,
# #这里一定要join拼接一下 转成字符串
text_cut  =  '/'.join(word_list)
#
#看一下连接后的样子
#关键点 text_cut 是词云要处理的内容
print(text_cut)
wordcloud = WordCloud(  background_color='white',font_path = 'msyh.ttc', width=1000, height=860, margin=2).generate(text_cut)
# 显示图片
plt.imshow(wordcloud)
plt.axis('off')
plt.show()