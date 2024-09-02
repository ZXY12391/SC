


if __name__=='__main__':
    p = r'E:\RZ项目\数据集问题\立场\RZ_post_4.csv'
    import pandas as pd
    data = pd.read_csv(p)
    data.to_csv('./data_4.csv', encoding='utf-8-sig')