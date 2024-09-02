from transformers import BertTokenizer,BertModel,pipeline,RobertaTokenizer,RobertaModel
import torch
import numpy as np
from mongo import action_transformer_test_task_oper


#feature_extract=pipeline('feature-extraction',model='./roberta_base',tokenizer='./roberta_base',padding=True,truncation=True,max_length=50, add_special_tokens = True,device=2)
feature_extract=pipeline('feature-extraction',model='roberta-base',tokenizer='roberta-base',padding=True,truncation=True,max_length=50, add_special_tokens = True)

def padding(tensor, row=100):
    """
    :param tensor: np array,
    :param row: how many row the matrix owns
    :return: padding tensor
    """
    shape = tensor.shape
    tensor_row, tensor_line = shape
    if row == tensor_row:
        return tensor.numpy()
    if row > tensor_row:
        # 获取缺的行数
        row = row - tensor_row
        temp = [0]*tensor_line
        temp = [temp for i in range(row)]
        # 生产补全的array
        np_arr = np.array(temp)
        # 将两个array拼接在一起
        tensor = np.vstack((tensor, np_arr))
        return tensor
    return tensor

def process_pne(id):
    # data = action_transformer_test_task_oper_post.select_datas(id)
    # data=[_['content'] for _ in data]
    # return data[0:100]
    data=action_transformer_test_task_oper.get_datas_by_id(id)[0]['tweet']
    if not data:
        return []
    if len(data)>=100:
        return data[0:100]
    else:
        return data

def process_one(id):
    tweets=process_pne(id)
    tweets_seq=[]
    if not tweets:
        for i in range(100):
            tweets_seq.append(torch.zeros(768))
    for tweet in tweets:
        try:
            tweet_tensor = torch.tensor(feature_extract(tweet)).squeeze(0)
            tweet_tensor = torch.mean(tweet_tensor, dim=0)
            tweets_seq.append(tweet_tensor)
        except Exception as e:
            print(e)
            tweets_seq.append(torch.zeros(768))
            print(id)
            pass
    print('process done one!')
    d={}
    d[id]=padding(torch.stack(tweets_seq,dim=0))
    return d

if __name__=='__main__':
    import time
    # a=process_one(ids[0])
    # print(a)
    # ids=action_transformer_test_task_oper.get_datas()
    # ids=[_['ID'] for _ in ids]
    # print(ids)
    # task={}
    # for t in ids:
    #     task=0
    # np.save('./process_task.npy',task)
    data=np.load('./process_task.npy', allow_pickle=True).item()
    print(data)
    user_idx=list(data.keys())
    print(len(user_idx))
    save_list = [user_idx[i:i + 5000] for i in range(0, len(user_idx), 5000)]
    c=0
    for temp_list in save_list:
        start_time = time.time()  # 记录程序开始执行的当前时间
        from multiprocessing.dummy import Pool
        pool = Pool(20)
        res = pool.map(process_one,temp_list)
        d = {}
        for i in range(len(res)):
            # print(ids_[i])
            # print(ids[i])
            id=list(res[i].keys())[0]
            # print(type(res[i]))
            d[id]=res[i][id]
        stop_time = time.time()  # 记录执行结束的当前时间
        func_time = stop_time - start_time  # 得到中间功能的运行时间
        np.save('./tweets_data_{}.npy'.format(c), d)
        print("func is running %s s" % func_time)
        c+=1

    pass