##############################################################
##collect dayily concept info
##############################################################
#data_times = pd.date_range('8/1/2017', periods=1, freq='D')
#concept_infos = collect_concept_volume_price(data_times)
#for c_name, c_info in concept_infos.items():
#    if len(c_info) > 3:
#        print "=================================================================S"
#        print c_name
#        print json.dumps(c_info, encoding="UTF-8", ensure_ascii=False, indent = 4)
#        print "=================================================================E"
##############################################################

#===========================================================
#print json.dumps(concept_infos, encoding="UTF-8", ensure_ascii=False, indent = 4)
#stock_infos = get_stock_volumes(MARKET_ALL, data_times)
#print stock_infos.sort_values(by = 'turnover')
#print is_sub_new_stock('20170525')
#print is_trading_day("2017-12-26") 
#print is_trading_day("2017-12-03") 
#print is_after_release('600476', '2017-12-26')
#engine = create_engine('mysql://root:123456@localhost/stock?charset=utf8')
#stock_turnover_rates.to_sql('turnover',engine,if_exists='replace',index=False) 
#==============================================
