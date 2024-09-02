
from mongodb import user_oper,keyword_post_oper

if __name__=='__main__':
    p=r'E:/RZ项目/RZ/2.xlsx'
    import pandas as pd
    data = pd.read_excel(io=p)
    data = data.loc[:, ["user_url"]]
    x=[]
    for each in data.user_url:
        x.append(each)
    y=[]
    for i in range(len(x)):
        if i<1634:
            continue
        y.append(x[i])
    print(len(y))
    for i in range(len(y)):
        # if i<100:
        #     continue
        test=y[i]
        data1=keyword_post_oper.find(test)
        print(data1)
        data2 = pd.DataFrame(data=data1, columns=list(data1[0].keys()))
        # PATH为导出文件的路径和文件名
        data2.to_csv('./data_0927/{}.csv'.format(test.replace('https://twitter.com/','')),encoding='utf-8-sig')
