# -*- coding: utf-8 -*-
"""
Created on 201906
Author : GJ
Python version：3.7
"""

import pandas as pd
from WindPy import *
import numpy as np
import xlsxwriter as xls

import datetime
import calendar
import copy
import os
from decimal import Decimal
import time
import seaborn as sns
import openpyxl

sns.set_style("white")

# 文件路径，无需设置，自动获取本程序所在文件夹
path = os.getcwd() + '\\'

# database file name
database_name = 'CN_Stock_SHHKconnect_Northbound_PyInput.xlsm'
old_output_name='CN_Stock_SHHKconnect_Northbound_PyOutput.xlsx'

# 定位某一日期在日期序列中的位置
# sele_date为选定日期
# date_range 为日期序列
# flag为参数，1为开始日期，2为结束日期
def anchor_se_date(sele_date, date_range, flag):
    asd = 0
    date_range = date_range.reset_index(drop=True)
    range_len = len(date_range)
    if type(sele_date) != type(date_range.iloc[0]):
        for n in range(0, range_len):
            str_d = str(date_range.iloc[n])
            str_d = str_d.replace("-", "")
            str_d = int(str_d[0:8])
            date_range.iloc[n] = str_d

    if sele_date <= date_range.iloc[0]:
        asd = 0
    elif sele_date >= date_range.iloc[range_len - 1]:
        asd = range_len - 1
    else:
        for m in range(0, range_len - 1):
            if flag == 1:
                if sele_date > date_range.iloc[m] and sele_date <= date_range.iloc[m + 1]:
                    asd = m + 1
                    break
            elif flag == 2:
                if sele_date >= date_range.iloc[m] and sele_date < date_range.iloc[m + 1]:
                    asd = m
                    break
    return asd


# 生成指标名函数
def result_col_str(row_no):
    result_cs = pd.DataFrame(data=None, index=range(0, row_no), columns=['indicator_name', 'unit', 'eng_name'])
    result_cs.iloc[0, 0] = 'indicator_name'
    result_cs.iloc[0, 1] = 'unit'
    result_cs.iloc[0, 2] = 'eng_name'
    result_cs2 = pd.DataFrame(data=None, index=range(0, row_no), columns=['indicator_name', 'unit', 'eng_name'])
    result_cs_col = pd.DataFrame(data=None, index=['c'], columns=['indicator_name', 'unit', 'eng_name'])
    result_cs_col.iloc[0, 0] = 'indicator_name'
    result_cs_col.iloc[0, 1] = 'unit'
    result_cs_col.iloc[0, 2] = 'eng_name'
    return result_cs, result_cs2, result_cs_col


#将日期格式转化为整形格式
def date_to_int_m(date_range, col):
    str_mon = str(date_range.iloc[col])
    str_mon = str_mon.replace("-", "")
    str_month = int(str_mon[4:6])
    str_year = int(str_mon[0:4])
    str_day = int(str_mon[6:8])
    str_date = int(str_mon[0:8])
    return str_date, str_year, str_month, str_day


def clear_df(df_data,row_no,col_no):
    clear_df = df_data.iloc[row_no:df_data.shape[0], col_no:df_data.shape[1]]
    clear_df = clear_df.reset_index(drop=True)
    clear_df = clear_df.T.reset_index(drop=True).T
    return clear_df


def copy_df(v_data,col_count):
    copy_df=pd.DataFrame(data=None,index=range(0,len(v_data)),columns=range(0,col_count))

    for col in range(0,col_count):
        copy_df.iloc[:,col]=v_data

    return copy_df


def cal_simple_index(holdshare,close,adj,ind,cs_i,eight_sector_dict,main_index_dict,date_range,mom_range,anchor_date,csi_index,index_chn_str,index_eng_str,index_unit_str):
    hs_data = clear_df(holdshare, 2, 1)
    # hs_data.fillna(0, inplace=True)
    date_range = holdshare.iloc[0, 1:holdshare.shape[1]]
    date_range = date_range.reset_index(drop=True)

    long_range = close.iloc[0, 1:close.shape[1]]
    long_range = long_range.reset_index(drop=True)

    vertical_range = close.iloc[0, 2:close.shape[1]]
    vertical_range = vertical_range.reset_index(drop=True)

    close_data = clear_df(close, 2, 1)
    close_data.fillna(0, inplace=True)


    hs_long = pd.DataFrame(data=None,index=range(0,close_data.shape[0]),columns=range(0,close_data.shape[1]))

    n=0
    for m in range (0,close_data.shape[1]):
        mx=date_range[n]-long_range[m]
        if mx==0:
            hs_long.iloc[:,m]=hs_data.iloc[:,n]
            n=n+1
        elif mx>0:
            hs_long.iloc[:,m]=hs_data.iloc[:,n-1]

    hs_now_bool=clear_df(hs_long,0,1)
    # hs_long_data.fillna(0, inplace=True)
    hs_now_bool[hs_now_bool.notna()] = 1
    hs_now_bool.fillna(0, inplace=True)


    hs_yestoday_data = hs_long.iloc[0:hs_long.shape[0], 0:hs_long.shape[1] - 1]
    hs_yestoday_data = hs_yestoday_data.reset_index(drop=True)
    hs_yestoday_data = hs_yestoday_data.T.reset_index(drop=True).T
    hs_yestoday_bool = hs_yestoday_data.copy()
    hs_yestoday_data.fillna(0, inplace=True)
    hs_yestoday_bool[hs_yestoday_bool.notna()] = 1
    hs_yestoday_bool.fillna(0, inplace=True)


    close_now_data=clear_df(close,2,2)
    close_now_data.fillna(0, inplace=True)

    close_yestoday_data = close.iloc[2:close.shape[0], 1:close.shape[1] - 1]
    close_yestoday_data = close_yestoday_data.reset_index(drop=True)
    close_yestoday_data = close_yestoday_data.T.reset_index(drop=True).T

    adj_now_data=clear_df(adj,2,2)
    adj_now_data.fillna(0, inplace=True)

    adj_yestoday_data = adj.iloc[2:adj.shape[0], 1:adj.shape[1] - 1]
    adj_yestoday_data = adj_yestoday_data.reset_index(drop=True)
    adj_yestoday_data = adj_yestoday_data.T.reset_index(drop=True).T


    numerator=hs_yestoday_bool*hs_now_bool*adj_now_data*close_now_data*hs_yestoday_data/adj_yestoday_data
    denominator=hs_yestoday_bool*hs_now_bool*hs_yestoday_data*close_yestoday_data

    numerator.fillna(0,inplace=True)
    denominator.fillna(0,inplace=True)
    num_all=cal_vertical_archi(numerator, ind, cs_i, eight_sector_dict, main_index_dict, vertical_range, vertical_range, index_chn_str,index_eng_str,index_unit_str,csi_index)
    denom_all=cal_vertical_archi(denominator, ind, cs_i, eight_sector_dict, main_index_dict, vertical_range, vertical_range, "北上资金周度指数_",'North_Index_W_','点',csi_index)

    num_data=clear_df(num_all,1,3)
    denom_data=clear_df(denom_all,1,3)
    denom_data [denom_data==0]=np.nan
    dod=num_data/denom_data
    dod.fillna(1, inplace=True)
    # dod= numerator.sum()/denominator.sum()
    index_all=pd.DataFrame(data=None,index=range(0,dod.shape[0]),columns=range(0,dod.shape[1]+1))

    index_all.iloc[:,0]=100
    for p in range(1, dod.shape[1] + 1):
        index_all.iloc[:, p] = index_all.iloc[:, p - 1] * dod.iloc[:, p - 1]

    for anchor_no in range(1,len(long_range)):
        if (long_range[anchor_no-1])<=anchor_date[0] and (long_range[anchor_no])>anchor_date[0]:
            anchor_index=anchor_no-1
            break

    anchor_df=index_all.iloc[:,anchor_index]
    code_df = pd.concat([anchor_df] * (index_all.shape[1]), axis=1)
    code_df = code_df.T.reset_index(drop=True).T
    code_df = code_df.reset_index(drop=True)

    # 拼接旧基准
    old_index_df=pd.concat([anchor_date[1]]* (index_all.shape[1]), axis=1)
    old_index_df=old_index_df.T.reset_index(drop=True).T
    old_index_df = old_index_df.reset_index(drop=True)

    anchor_index_df=index_all/code_df*old_index_df

    right_col = pd.DataFrame(data=long_range).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')

    right_data = pd.concat([right_col,anchor_index_df])

    mon_left=num_all.iloc[:,0:3]
    # week_left=denom_all.iloc[:,0:3]

    mon_all=pd.concat([mon_left,right_data],axis=1)
    # week_all=pd.concat([week_left,right_data],axis=1)

    mon_result=slice_fmkt(mon_all,mom_range)
    # week_result=slice_fmkt(week_all,wow_range)
    return mon_result


def cal_idex(holdshare,vwap,div_df,close):
    hs_data = clear_df(holdshare, 2, 2)
    hs_data.fillna(0, inplace=True)

    close_data=clear_df(close,2,2)
    close_data.fillna(0,inplace=True)


    hs_data_bool = clear_df(holdshare, 2, 2)
    hs_data_bool[hs_data_bool.notna()] = 1
    hs_data_bool.fillna(0, inplace=True)

    vwap_data = clear_df(vwap, 2, 2)
    vwap_data.fillna(0, inplace=True)

    hs_yestoday_data = holdshare.iloc[2:holdshare.shape[0], 1:holdshare.shape[1] - 1]
    hs_yestoday_data = hs_yestoday_data.reset_index(drop=True)
    hs_yestoday_data = hs_yestoday_data.T.reset_index(drop=True).T
    hs_yestoday_bool = hs_yestoday_data.copy()
    hs_yestoday_data.fillna(0, inplace=True)

    close_yestoday_data=close.iloc[2:close.shape[0], 1:close.shape[1] - 1]
    close_yestoday_data=close_yestoday_data.reset_index(drop=True)
    close_yestoday_data = close_yestoday_data.T.reset_index(drop=True).T

    hs_yestoday_bool[hs_yestoday_bool.notna()] = 1
    hs_yestoday_bool.fillna(0, inplace=True)
    # 两个bool变量是为了做到可比口径，也为了避免wind数据错误
    delta_data = (hs_data - hs_yestoday_data * div_df) * vwap_data * hs_data_bool * hs_yestoday_bool

    return delta_data



def cal_ind_flow(holdshare,vwap,div_df):
    hs_data=clear_df(holdshare,2,2)
    hs_data.fillna(0, inplace=True)

    hs_data_bool=clear_df(holdshare,2,2)
    hs_data_bool[hs_data_bool.notna()]=1
    hs_data_bool.fillna(0, inplace=True)

    vwap_data=clear_df(vwap,2,2)
    vwap_data.fillna(0, inplace=True)


    hs_yestoday_data = holdshare.iloc[2:holdshare.shape[0], 1:holdshare.shape[1]-1]
    hs_yestoday_data = hs_yestoday_data.reset_index(drop=True)
    hs_yestoday_data = hs_yestoday_data.T.reset_index(drop=True).T
    hs_yestoday_bool = hs_yestoday_data.copy()
    hs_yestoday_data.fillna(0, inplace=True)

    hs_yestoday_bool[hs_yestoday_bool.notna()] = 1
    hs_yestoday_bool.fillna(0, inplace=True)
    # 两个bool变量是为了做到可比口径，也为了避免wind数据错误
    delta_data = (hs_data-hs_yestoday_data*div_df)*vwap_data*hs_data_bool*hs_yestoday_bool

    return delta_data


def cal_vertical_archi(delta_data,ind,dict,eight_sector_dict,main_index_dict,week_range,write_date_range,chn_str,eng_str,unitstr,csi_index):

    code_data = pd.DataFrame(data=None, index=range(0, delta_data.shape[0]), columns=range(0, len(week_range)))

    ind_col_select = select_data(ind,week_range)
    ind_data_clear= clear_df(ind,2,1)
    ind_data = ind_data_clear.iloc[:,ind_col_select[0]]
    ind_data = clear_df(ind_data,0,0)


    left_df = result_col_str(dict.shape[0])
    left_df_main_index=result_col_str(main_index_dict.shape[0])
    left_df_eight = result_col_str(eight_sector_dict.shape[0])

    right_col = pd.DataFrame(data=write_date_range).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')
    right_data = pd.DataFrame(data=0, index=range(0, dict.shape[0]), columns=range(0, len(week_range)))
    right_data_main_index = pd.DataFrame(data=0, index=range(0, main_index_dict.shape[0]), columns=range(0, len(week_range)))
    right_data_eight = pd.DataFrame(data=0, index=range(0, eight_sector_dict.shape[0]), columns=range(0, len(week_range)))

    code_list=ind.iloc[2:ind.shape[0],0]

    left_df_main_index[1].iloc[0, 0] = chn_str + main_index_dict.iloc[0, 0]
    left_df_main_index[1].iloc[0, 1] = unitstr
    left_df_main_index[1].iloc[0, 2] = eng_str + main_index_dict.iloc[0, 1]

    right_data_main_index.iloc[0,:]= delta_data.sum()/100000000

    r_style=['','^6','^(0|3)','^00(0|1)','^00(2|3)','^3']
    csi_index_no=0
    for main_no in range(1,main_index_dict.shape[0]):
        if main_index_dict.iloc[main_no,1][0:3]=='CSI':
           csi_col_select = select_data(csi_index[csi_index_no], week_range)
           csi_clear_now=clear_df(csi_index[csi_index_no],2,1)
           csi_data = csi_clear_now.iloc[:, csi_col_select[0]]
           csi_data = clear_df(csi_data, 0, 0)
           csi_data[csi_data!="是"]=0
           csi_data[csi_data == "是"] = 1
           code_df=csi_data

           csi_index_no=csi_index_no+1

        else:
            code_list_now = code_list.copy()
            code_list_now[code_list_now.str.contains(r_style[main_no])]=1
            code_df = pd.concat([code_list_now] * (delta_data.shape[1]), axis=1)
            code_df = code_df.T.reset_index(drop=True).T
            code_df = code_df.reset_index(drop=True)
            code_df[code_df != 1] = 0

        left_df_main_index[1].iloc[main_no, 0] = chn_str + main_index_dict.iloc[main_no, 0]
        left_df_main_index[1].iloc[main_no, 1] = unitstr
        left_df_main_index[1].iloc[main_no, 2] = eng_str + main_index_dict.iloc[main_no, 1]

        main_data=delta_data*code_df
        right_data_main_index.iloc[main_no, :] = main_data.sum() / 100000000

    for cs_no in range(0, dict.shape[0]):
        now_ind_df = ind_data.copy()
        now_ind_df[now_ind_df == dict.iloc[cs_no, 2]] = 1
        now_ind_df[now_ind_df != 1] = 0
        # testx=now_ind_df*delta_data/1000000
        # amx=testx[testx!=0]
        # bmx=amx.dropna(how='all')
        right_data.iloc[cs_no, :] = (now_ind_df * delta_data).sum() / 100000000

        left_df[1].iloc[cs_no, 0] = chn_str + dict.iloc[cs_no, 1]
        left_df[1].iloc[cs_no, 1] = unitstr
        left_df[1].iloc[cs_no, 2] = eng_str + dict.iloc[cs_no, 3]

        am = eight_sector_dict[eight_sector_dict['FOUR_SECTOR'] == dict.iloc[cs_no, 0]].index.tolist()
        if am!=[]:
            right_data_eight.iloc[am[0],:]=right_data_eight.iloc[am[0],:]+right_data.iloc[cs_no,:]

            left_df_eight[1].iloc[am[0], 0] = chn_str + eight_sector_dict.iloc[am[0], 0]
            left_df_eight[1].iloc[am[0], 1] = unitstr
            left_df_eight[1].iloc[am[0], 2] = eng_str + eight_sector_dict.iloc[am[0], 1]

    main_index_result=pd.concat([left_df_main_index[1],right_data_main_index],axis=1)
    eight_result=pd.concat([left_df_eight[1],right_data_eight],axis=1)
    cs_result=pd.concat([left_df[1],right_data],axis=1)
    col_result=pd.concat([left_df[2],right_col],axis=1)

    result=pd.concat([main_index_result,eight_result,cs_result])
    result=result.reset_index(drop=True)
    f_result=pd.concat([col_result,result])
    return f_result


# 合并压缩数据
def cal_push_data(delta_data,ind,dict,eight_sector_dict,main_index_dict,week_range,date_range,chn_str,eng_str,edb,csi_index):

    #sector_day_data = pd.DataFrame(data=0, index=range(0, dict.shape[0]), columns=range(0, delta_data.shape[1]))

    day_data=cal_vertical_archi(delta_data,ind,dict,eight_sector_dict,main_index_dict,date_range,date_range,chn_str,eng_str,'亿元',csi_index)
    sector_day_data=clear_df(day_data,1,3)

    right_col = pd.DataFrame(data=None, index=['c'], columns=range(0, len(week_range[0])))
    right_data = pd.DataFrame(data=0, index=range(0, sector_day_data.shape[0]), columns=range(0, len(week_range[0])))
    edb_col_select=select_data(edb,date_range)
    edb_clear_data=clear_df(edb,2,1)
    edb_clear_select = edb_clear_data.iloc[:,edb_col_select[0]]
    edb_clear = clear_df(edb_clear_select,0,0)

    sector_day_data.iloc[0:edb_clear.shape[0],0:edb_clear.shape[1]]=edb_clear

    for week_no in range(0, len(week_range[0])):
        right_col.iloc[0, week_no] = str(week_range[2][week_no])
        for day_no in range(0, len(date_range)):
            if week_range[1][week_no] < date_range[day_no] and week_range[1][week_no + 1] >= date_range[day_no]:
                right_data.iloc[:, week_no] = right_data.iloc[:, week_no] + sector_day_data.iloc[:, day_no]
            elif week_range[1][week_no + 1] < date_range[day_no]:
                break

    result_left = day_data.iloc[0:day_data.shape[0],0:3]

    result_right = pd.concat([right_col, right_data])
    result_df = pd.concat([result_left, result_right], axis=1)
    return result_df


# 计算分红拆股df
def cal_div_df(divcap,exdate,date_range):
    divcap_data=divcap.iloc[2:divcap.shape[0],1:divcap.shape[1]]
    divcap_data=divcap_data.reset_index(drop=True)
    divcap_data = divcap_data.T.reset_index(drop=True).T

    exdate_data = exdate.iloc[2:exdate.shape[0], 1:exdate.shape[1]]
    exdate_data = exdate_data.reset_index(drop=True)
    exdate_data = exdate_data.T.reset_index(drop=True).T

    divcap_data.fillna(0,inplace=True)
    exdate_data.fillna(0,inplace=True)

    div_df= pd.DataFrame(data=1,index=range(0,divcap_data.shape[0]),columns=range(0,len(date_range)))

    date_df=copy_df(date_range,divcap_data.shape[0]).T
    for div_col in range(0,divcap_data.shape[1]):
        cap_df=copy_df(divcap_data.iloc[:,div_col],len(date_range))
        exdate_df=copy_df(exdate_data.iloc[:,div_col],len(date_range))
        delta_date_df=date_df - exdate_df
        product_df= pd.DataFrame(data=0,index=range(0,divcap_data.shape[0]),columns=range(0,len(date_range)))
        for m in range(0,delta_date_df.shape[0]):
            if delta_date_df.iloc[m,0]<=0 and delta_date_df.iloc[m,delta_date_df.shape[1]-1]>=0 :
                for n in range(0,delta_date_df.shape[1]):
                    if n==0:
                       if delta_date_df.iloc[m,n]==0:
                           product_df.iloc[m,n]=cap_df.iloc[m,n]
                           break
                    else:
                       if delta_date_df.iloc[m, n] == 0:
                           product_df.iloc[m, n] = cap_df.iloc[m, n]
                           break
                       elif   delta_date_df.iloc[m, n-1] < 0   and delta_date_df.iloc[m, n] > 0 :
                           product_df.iloc[m, n] = cap_df.iloc[m, n]
                           break

        div_df=div_df+product_df
    return div_df


# 从日度数据截取周度、月度数据
def select_data(holdshare,mom_range):
    hs_date = holdshare.iloc[0, 1:holdshare.shape[1]]
    hs_date = hs_date.reset_index(drop=True)
    sele_date=[]
    for m in range(0,len(mom_range)):
        divid_date=hs_date-mom_range[m]
        for n in range(0,len(divid_date)):
            if n==0:
                if divid_date[n]==0:
                   sele_date.append(n)
                   break
            else:
                if divid_date[n]==0:
                    sele_date.append(n)
                    break
                elif divid_date[n-1]<0 and divid_date[n]>0:
                    sele_date.append(n-1)
                    break
                elif divid_date[len(divid_date)-1]<0  and n==len(divid_date)-1:
                    sele_date.append(n)

    return sele_date,hs_date[sele_date]


# 计算持仓
def cal_hold(holdshare,close,mom_range):

    hs_col_list=select_data(holdshare,mom_range[0])
    close_col_list=select_data(close,mom_range[0])

    hs_data = clear_df(holdshare, 2, 1)
    hs_data.fillna(0, inplace=True)

    close_data = clear_df(close,2,1)
    close_data.fillna(0,inplace=True)

    hs_sele_data=hs_data.iloc[:,hs_col_list[0]]
    close_sele_data=close_data.iloc[:,close_col_list[0]]

    hs_sele_data_clear = clear_df(hs_sele_data,0,0)
    close_sele_data_clear =clear_df(close_sele_data,0,0)

    hold_data=hs_sele_data_clear*close_sele_data_clear

    return hold_data


def cal_float_mkt(fmk,cs_i,eight_sector_dict,main_index):

    fmk_data=clear_df(fmk,2,1)
    fmk_include_code=clear_df(fmk,2,0)
    # 43为静态量，如果统计数量有变，则需要手动修改
    fmk_result=pd.DataFrame(data=None, index=range(0,46), columns=range(0, fmk_data.shape[1]))

    fmk_result.iloc[0:9,:]= fmk_data.iloc[0:9,:]/100000000
    left_df = result_col_str(46)

    left_df[1].iloc[0:9,0]= '流通市值_' + main_index.iloc[:,0]
    left_df[1].iloc[0:9, 1] = '亿元'
    left_df[1].iloc[0:9, 2] = 'Floating_MarktCap_' + main_index.iloc[:, 1]

    right_col = pd.DataFrame(data=fmk.iloc[0,1:fmk.shape[1]]).T
    right_col = right_col.T.reset_index(drop=True).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')


    for m in range(0,eight_sector_dict.shape[0]):
        now_ind  = eight_sector_dict.iloc[m, 0]
        cs_now = cs_i[cs_i.CS_I_FS==now_ind]
        sector_merge=pd.merge(cs_now, fmk_include_code, how='left', left_on='CS_I_SECTOR', right_on=0)
        sector_merge_clear=clear_df(sector_merge,0,5)

        left_df[1].iloc[9 + m,0]='流通市值_' + eight_sector_dict.iloc[m,0]
        left_df[1].iloc[9 + m, 1] = '亿元'
        left_df[1].iloc[9 + m, 2]='Floating_MarktCap_' + eight_sector_dict.iloc[m, 1]
        fmk_result.iloc[9 + m, :] = sector_merge_clear.sum()/100000000

    for n in range(0,cs_i.shape[0]):
        now_ind = cs_i.iloc[n, 2]

        cs_now = pd.DataFrame(data=now_ind,index=[0],columns=['CS_I_SECTOR'])
        sector_merge = pd.merge(cs_now, fmk_include_code, how='left', left_on='CS_I_SECTOR', right_on=0)
        sector_merge_clear = clear_df(sector_merge, 0, 2)

        left_df[1].iloc[17 + n, 0] = '流通市值_' + cs_i.iloc[n, 1]
        left_df[1].iloc[17 + n, 1] = '亿元'
        left_df[1].iloc[17 + n, 2] = 'Floating_MarktCap_' + cs_i.iloc[n, 3]
        fmk_result.iloc[17 + n, :] = sector_merge_clear.sum()/100000000

    cs_result = pd.concat([left_df[1], fmk_result], axis=1)
    col_result = pd.concat([left_df[2], right_col], axis=1)

    f_result = pd.concat([col_result, cs_result])
    return f_result,fmk_result


def cal_fund_index(edb,date_range,mom_range,chnstr,engstr,unitstr):
    edb_col_select = select_data(edb, date_range)
    edb_clear_data = clear_df(edb, 2, 1)
    edb_clear_select = edb_clear_data.iloc[:, edb_col_select[0]]
    edb_clear = clear_df(edb_clear_select, 0, 0)

    left_df = result_col_str(1)
    left_df[1].iloc[0, 0] = chnstr
    left_df[1].iloc[0, 1] = unitstr
    left_df[1].iloc[0, 2] = engstr

    right_col = pd.DataFrame(data=date_range).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')

    right=pd.concat([right_col,edb_clear])

    right=right.iloc[:,2:right.shape[1]]
    right=right.T.reset_index(drop=True).T

    left=pd.concat([left_df[2],left_df[1]])
    full_result=pd.concat([left,right],axis=1)

    slice_result=slice_fmkt(full_result,mom_range)

    return slice_result


def slice_fmkt(fmkt,mom_range):
    fmkt_bridge=clear_df(fmkt,0,2)
    sele_col_list=select_data(fmkt_bridge,mom_range[0])
    fmkt_data=clear_df(fmkt,1,3)
    mkt_sele_data=fmkt_data.iloc[:,sele_col_list[0]]
    mkt_sele_data_clear=clear_df(mkt_sele_data,0,0)
    right_col = pd.DataFrame(data=mom_range[2]).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')
    right_data = pd.DataFrame(data=mkt_sele_data_clear, index=range(0, mkt_sele_data_clear.shape[0]), columns=range(0, len(mom_range[2])))

    left=fmkt.iloc[:,0:3]
    right = pd.concat([right_col,right_data])

    return pd.concat([left,right],axis=1)


def slice_index(index_data,mom_range,chnstr,engstr):
    hs_col_list = select_data(index_data, mom_range[0])

    index_clear=clear_df(index_data,1,1)
    index_sele_data=index_clear.iloc[:,hs_col_list[0]]
    index_sele_data_clear=clear_df(index_sele_data,0,0)

    left_df = result_col_str(1)

    right_col = pd.DataFrame(data=mom_range[2]).T
    right_col = right_col.rename({right_col.index[0]: 'c'}, axis='index')
    right_data = pd.DataFrame(data=index_sele_data_clear, index=range(0, 1), columns=range(0, len(mom_range[2])))

    left_df[1].iloc[0,0]='北上资金' + chnstr
    left_df[1].iloc[0, 1] = '点'
    left_df[1].iloc[0, 2] = 'North' + engstr

    up_result=pd.concat([left_df[2],right_col],axis=1)
    donw_result=pd.concat([left_df[1],right_data],axis=1)

    result=pd.concat([up_result,donw_result])
    return result


# 指数定基2017/1/31
# 将数据结果写入excel文件
def write_result_excel(pys_result, workbook, worksheetname):
    worksheet = workbook.add_worksheet(worksheetname)
    worksheet.set_column('A:A', 28)
    worksheet.set_column('B:B', 8)
    worksheet.set_column('C:C', 20)
    # 设定格式
    format_column = workbook.add_format(
        {'text_wrap': False, 'font_name': 'Times New Roman', 'font_size': 10, 'align': 'vcenter'})
    format_decimal = workbook.add_format(
        {'text_wrap': False, 'font_name': 'Times New Roman', 'font_size': 10, 'num_format': '#,##0.0_ ',
         'align': 'vcenter'})
    format_date = workbook.add_format(
        {'text_wrap': False, 'font_name': 'Times New Roman', 'font_size': 10, 'num_format': 'yyyymmdd',
         'align': 'vcenter'})
    pys_result = pys_result.where(pys_result.notnull(), '')
    # 写入列名
    for date_col in range(3, pys_result.shape[1]):
         this_date=pys_result.iloc[0, date_col]
         if isinstance(this_date,int)==False :
             this_date=int(this_date.strftime('%Y%m%d'))

         date_str =datetime.datetime.strptime(str(this_date), '%Y%m%d')

         worksheet.write_datetime(0, date_col, date_str, format_date)
    # 写入列
    for r_col in range(0, pys_result.shape[1]):
        if r_col < 3:
            worksheet.write_column(1, r_col, pys_result.iloc[1:pys_result.shape[0], r_col], format_column)
        else:
            worksheet.write_column(1, r_col, pys_result.iloc[1:pys_result.shape[0], r_col], format_decimal)

    worksheet.freeze_panes('D2')


# 为输出到同一excel，删除第一行列名数据
def del_colname(data_result):
    data_f=data_result.iloc[1:data_result.shape[0],:]
    return data_f


def crop_week(week_range,date_range):
    for m in range(0,len(week_range)):
        if week_range[m]>=date_range[0]:
            break

    for n in range(0,len(week_range)):
        if week_range[n]>=date_range[len(date_range)-1]:
            break

    crop_week=week_range[m:n+1]
    crop_week=crop_week.reset_index(drop=True)
    crop_week3=crop_week.copy()
    crop_week3[len(crop_week3)-1]=date_range[len(date_range)-1]
    crop_week2=week_range[m-1:n+1]
    crop_week2 = crop_week2.reset_index(drop=True)
    return crop_week,crop_week2,crop_week3


def  find_last_col(last_date,result_df_b):
     last_date_int=int(last_date.strftime('%Y%m%d'))
     for m in range(3,result_df_b.shape[1]):
         if result_df_b.iloc[0,m]==last_date_int:
             new_col=m+1
             break
     return new_col


# 生成结果存放excel文件，保存目录为程序运行同目录，命名方式为：Result_ + 保存时时间.xlsx
workbook = xls.Workbook(
    path + 'CN_Stock_SHHKconnect_Northbound_PyOutput' + '.xlsx',
    {'nan_inf_to_errors': True})   #time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())) +
# 读取板块列表文件

old_week_df=pd.read_excel(path+old_output_name,'Python_Week',header=None)
old_month_df=pd.read_excel(path+old_output_name,'Python_Month',header=None)

eng_array=old_week_df.iloc[:,2]

eng_list=[]
eng_logi=eng_array.str.contains('_Index_')
for m in range(1,len(eng_array)):
    if eng_logi[m]==True :
        eng_list.append(m)

month_anchor_index=old_month_df.iloc[eng_list,old_month_df.shape[1]-2]
week_anchor_index=old_week_df.iloc[eng_list,old_week_df.shape[1]-2]
month_anchor_date=old_month_df.iloc[0,old_month_df.shape[1]-2].strftime('%Y%m%d')
week_anchor_date=old_week_df.iloc[0,old_week_df.shape[1]-2].strftime('%Y%m%d')
anchor_data_month=(int(month_anchor_date),month_anchor_index)
anchor_data_week=(int(week_anchor_date),week_anchor_index)

slice_old_mom=old_month_df.iloc[:,0:old_month_df.shape[1]-1]
slice_old_wow=old_week_df.iloc[:,0:old_week_df.shape[1]-1]

holdshare = pd.read_excel(path + database_name, 'holdshare', header=None)
close = pd.read_excel(path + database_name, 'close', header=None)
stock_adj = pd.read_excel(path + database_name, 'adjfactor', header=None)

vwap = pd.read_excel(path + database_name, 'vwap', header=None)
ind = pd.read_excel(path + database_name, 'ind', header=None)
fmk = pd.read_excel(path + database_name, 'Floating_MktCap', header=None)
dict = pd.read_excel(path + database_name, 'sector_list', header=0)
date_sheet=pd.read_excel(path + database_name, 'date', header=None)
fund=pd.read_excel(path + database_name, 'fund_index', header=None)
csi300=pd.read_excel(path + database_name, 'csi300', header=None)
csi500=pd.read_excel(path + database_name, 'csi500', header=None)
csi1000=pd.read_excel(path + database_name, 'csi1000', header=None)
csi_index=(csi300,csi500,csi1000)

week_range=date_sheet.iloc[2:date_sheet.shape[0],0]
week_range=week_range.reset_index(drop=True)

mon_range=date_sheet.iloc[2:date_sheet.shape[0],3]
mon_range=mon_range.reset_index(drop=True)

divcap = pd.read_excel(path + database_name, 'div_cap', header=None)
divexdate = pd.read_excel(path + database_name, 'div_exdate', header=None)
edb_data = pd.read_excel(path + database_name, 'edb', header=None)

date_range=holdshare.iloc[0,2:holdshare.shape[1]]
date_range=date_range.reset_index(drop=True)

date_range_long=close.iloc[0,2:close.shape[1]]
date_range_long=date_range_long.reset_index(drop=True)

cs_i = dict.loc[:, ['CS_I_FS', 'CS_I','CS_I_SECTOR', 'CS_I_ENG']]
eight_sector_dict = dict.loc[:,['FOUR_SECTOR','FS_ENG']]
main_index_dict = dict.loc[:,['Main_Index','Main_Index_Eng']]

cs_i=cs_i.dropna(how='all')
eight_sector_dict =eight_sector_dict.dropna(how='all')
main_index_dict=main_index_dict.dropna(how='all')
float_mkt = cal_float_mkt(fmk,cs_i,eight_sector_dict,main_index_dict)

wow_range=crop_week(week_range,date_range)
mom_range=crop_week(mon_range,date_range)
fmkt_month=slice_fmkt(float_mkt[0],mom_range)
fmkt_week=slice_fmkt(float_mkt[0],wow_range)
div_df=cal_div_df(divcap,divexdate,date_range)

fund_mindex=cal_fund_index(fund,date_range_long,mom_range,'中证股票基金月度指数','CSI_Stock_Fund_M_Index','点')
fund_windex=cal_fund_index(fund,date_range_long,wow_range,'中证股票基金周度指数','CSI_Stock_Fund_W_Index','点')

simple_index_month = cal_simple_index(holdshare,close,stock_adj,ind,cs_i,eight_sector_dict,main_index_dict,date_range,mom_range,anchor_data_month,csi_index,'北上资金月度指数_','North_Index_M','点')
simple_index_week  = cal_simple_index(holdshare,close,stock_adj,ind,cs_i,eight_sector_dict,main_index_dict,date_range,wow_range,anchor_data_week,csi_index,'北上资金周度指数_','North_Index_W','点')

hold_data_month = cal_hold(holdshare,close,mom_range)
result_hold_month = cal_vertical_archi(hold_data_month,ind,cs_i,eight_sector_dict,main_index_dict,mom_range[0],mom_range[2],'月度持仓_','North_Position_M_','亿元',csi_index)

hold_data_week = cal_hold(holdshare,close,wow_range)
result_hold_week = cal_vertical_archi(hold_data_week,ind,cs_i,eight_sector_dict,main_index_dict,wow_range[0],wow_range[2],'周度持仓_','North_Position_W_','亿元',csi_index)

delta_data = cal_ind_flow(holdshare, vwap, div_df)
ind_flow_wow=cal_push_data(delta_data,ind,cs_i,eight_sector_dict,main_index_dict,wow_range,date_range,'周度资金净流入_','North_Netinflow_W_',edb_data,csi_index)
ind_flow_mom=cal_push_data(delta_data,ind,cs_i,eight_sector_dict,main_index_dict,mom_range,date_range,'月度资金净流入_','North_Netinflow_M_',edb_data,csi_index)

wow_result=pd.concat([fund_windex,del_colname(simple_index_week),del_colname(ind_flow_wow) ,del_colname(result_hold_week),del_colname(fmkt_week)])
mom_result=pd.concat([fund_mindex,del_colname(simple_index_month),del_colname(ind_flow_mom),del_colname(result_hold_month),del_colname(fmkt_month)])

new_wow_result=clear_df(wow_result,0,find_last_col(old_week_df.iloc[0,old_week_df.shape[1]-2],wow_result))
new_mom_result=clear_df(mom_result,0,find_last_col(old_month_df.iloc[0,old_month_df.shape[1]-2],mom_result))

final_mom=pd.concat([slice_old_mom,new_mom_result],axis=1)
final_mom=clear_df(final_mom,0,0)

final_wow=pd.concat([slice_old_wow,new_wow_result],axis=1)
final_wow=clear_df(final_wow,0,0)

write_result_excel(final_mom, workbook, 'Python_Month')
write_result_excel(final_wow, workbook, 'Python_Week')
# write_result_excel(result_hold_month, workbook, 'Python_Month')
# write_result_excel(result_hold_week, workbook, 'Python_Week')
workbook.close()
print("Done!")
