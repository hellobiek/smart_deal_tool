from datetime import datetime 
def datetime_to_str(now=None):
    """
    把日期格式化成yyyy-mm-dd的形式
    """
    if now == None:
        now=datetime.now()
        return "{0:0>4}-{1:0>2}-{2:0>2}".format(now.year,now.month,now.day)
    else:
        return "{0:0>4}-{1:0>2}-{2:0>2}".format(now.year,now.month,now.day)
