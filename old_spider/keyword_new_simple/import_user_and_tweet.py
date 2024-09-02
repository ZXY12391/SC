
from mongodb import keyword_keyword_oper


flitter=['中共','安保','监狱','批评']

def isimportant(s):
    for f in flitter:
        if f in s['full_text']:
            return True
    return False

data=keyword_keyword_oper.find_all_two()

t=[]
u=[]

for d in data:
    if isimportant(d):
        t.append(d)
        u.append(d['user_url'])

for tt in t:
    print('-----')
    print(tt['tweet_url'])
    print(tt['full_text'])
    print(tt['user_url'])
    print('-----')
print(list(set(u)))