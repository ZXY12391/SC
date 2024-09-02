class ProxyOrNetworkException(Exception):
    pass


class NoSpiderAccountException(Exception):
    pass

if __name__=='__main__':
    #异常处理OK
    a=1
    if a==1:
        raise NoSpiderAccountException