# 开发计划
##2018-08-04
    - 组合的计算和获取，包括关注的组合以及所有的组合(done)
    - 每日数据的更新(done)
    - vpn的使用获取ticker数据(done)
##2018-08-07
    - futu软件定时删除日志(done)
    - 通达信数据的解析和验证(done)
    - 获取每天的tick信息，写入数据库(done)
##2018-08-12
    - 前复权价格的计算方法(done)
    - mac等，技术指标的实现(done)
    - 前复权价格的计算方法(done)
##2018-08-22
    - 平均股价的获取(done)
    - 涨跌停数据的获取(done)
##2018-08-24
    - 板块的分析数据的获取(done)
##2018-09-21
    - MAC和筹码分布的设计与实现(done)
    - KDJ的实现(done)
    - 股票还未上市，没有k线的处理(done)
##2018-10-10
    - 完成板块成庄的选股模型
        - 博弈K线的差(done)
        - 逆势大盘的程度(done)
        - 近邻筹码的密集程度(done)
    - 港股通持股(done)
    - 融资融券分析，分析融资融券的换手率(done)
    - MAC和筹码系统的实现(done)
##2018-11-01
    - 获取指数的成分股(done)
    - 沪深两市每天的成交状态(done)
##2018-11-12
    - 超跌反弹模型(done)
    - 牛熊股比模型(done)
    - 异步读写mysql(done)
    - 沪港通，深港通资金流向(done)
    - 失败的列表增加重试的功能(done)
    - 每周投资者增加数量的爬虫(done)
    - gevent和python多进程并发(done)
    - cmysql.py删除db和table确保成功(done)
##2019-02-25
    - 日常更新自动化(done)
##2019-03-01
    - 增加30分钟线和60分钟线(done)
    - 根据日线生成周线和月线(done)
    - 交割单复盘:交割单来自futu(done)
    - 技术指标的理解和实现(done)
##2019-03-17
    - 交易
        - 日常，周末复盘
    - 系统开发与维护
        - Redis服务不稳定问题(done)
        - 多进程莫名其妙的退出(done)
    - 量化实践
        - LDA的学习和实践
        - 随机深林完成，学习领回归和laso
    - 量化基础学习用品
        - LDA的基础学习
        - QDA的基础学习
        - SVM的基础学习
##2019-03-20
    - 专业知识
        - 交易
        - 日复盘和周复盘
    - 系统开发与维护
        - 板块牛熊股比(done)
        - log的重复输出(done)
    - 量化策略开发
        - MACD的背离开发
            - 掌握pyalgotrade框架
            - 将macd和pyalgotrade框架相结合
            - 策略：macd底背离买入，顶背离卖出
            - 回测模型
            - 分析结果
        - 网格策略开发
        - 沪深300的股指的回归和分类
        - 利用回声神经网络来预测股价: https://towardsdatascience.com/predicting-stock-prices-with-echo-state-networks-f910809d23d4
        - K线形态是否可以预测股价: http://mariofilho.com/can-machine-learning-model-predict-the-sp500-by-looking-at-candlesticks
        - 配资对股市的影响：把创业板全部融资融券标的拉出来，单独算它的两融余额，拉出随时间变化的曲线，分别取一阶和二阶导数，然后再对创业板综指，求一阶和二阶导。两个导数差，会组成两个开口，然后拿15年上半年的数据回测对比。
        - 股市是否符合markwic的资产投资组合理论
        - 使用机器学习来预测股价：https://medium.com/analytics-vidhya/using-machine-learning-to-predict-stock-prices-c4d0b23b029a
    - 机器学习
        - 基础数学复习
        - 线性回归(领回归,Laso)的学习和实践
        - LDA的学习和实践
        - QDA的学习和实践
        - SVM的学习和实践
        - 随机深林的学习和实践
        - XGboost和Adaboost的学习和实践
        - HMM的学习和实践
        - 遗传算法的学习和实践
##2019-03-27
    - 专业知识
        - 交易复盘
    - 系统开发与维护
        - 解决bug：
            1、并行处理时处理相同的stock
            2、相同的进程不能同时被启动两次
    - 量化策略开发
        - 熟悉pyalgorithm的broker和comission
    - 机器学习
        - 无
##TODO
    - 技术提升：
        - 反汇编技术
        - 机器学习，神经网络的学习和实践
    - 软件维护
        - 大宗交易爬虫
        - 基金数据分析爬虫
        - 爬虫数据爬取异常报警
        - 股票的贡献点数的计算
        - 富途股票突破限制的情况
        - 通达信数据获取失败后的处理
        - 需要将长城证券的broker修改为富途的broker的类型
        - 证券发行及登记和证券过户，代发现金红利和资金结算爬虫
        - 资金的每日资金分析（融资融券，港股通，基金，MSCI，十大股东等）
        - 自定义broker，使得broker可以计算每一只股票的收益情况，每一只股票的仓位和总体的仓位的区分。
    - 模型开发
        - KDJ放量买入模型
        - 一阳指战法的实现
        - 傍大款战法的代码与实现
        - 天线宝宝战法的代码实现
        - 板块组合的战法与实现,板块流通市值
    - 复盘应该包含的功能：
        - capital alalysis
            - marauder map, 板块和个股的活点地图
                - 潜龙状态
                - 见龙状态
                - 飞龙状态
                - 亢龙状态
            - 流动市值与总成交额
                - 流动市值分析
                - 总成交额
            - 成交额板块分析
                - 成交额板块排行
                - #成交额增量板块排行
                - #成交额减量板块排行
                - #涨幅排行
                - #跌幅排行
            - 指数点数贡献分析(not do now)
                - 按照个股排序
            - 成交额构成分析(not do now)
                - 融资融券资金
                - 沪港通资金
                - 涨停板资金
                - 基金仓位资金
                - 股票回购
                - 大宗交易
            - emotion alalysis
                - 大盘的情绪分析
        - plate alalysis
                - capital alalysis(not do now)
                    - 沪港通
                    - 融资融券
                    - 基金
                    - 回购
                    - 大宗
        - stock analysis
                - capital alalysis(not do now)
                    - 沪港通
                    - 融资融券
                    - 基金
                    - 回购
                    - 大宗交易
                - technical analysis
                    - chip alalysis
                       #逆势大盘
                       #90:3
                       #逆势飘红
                       #牛长熊短
                       #线上横盘
                       #博弈K线无量长阳
        - model running
            - model training
            - model evaluation 
            - model backtesting
            - model trading
