# 匯入套件
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
#%%
# 資料預處理
# 清理空值資料
# 讀取資料，從第一行開始往下讀取全部資料
data = pd.read_csv('rowdata1.csv',
                   header=0,nrows=286,encoding='ansi',thousands=',') # thousands -->轉成數值型態

# data.info() --> 看空值處理後之資料

# 新增一欄為「地區」
index = 0
area = []
# data['Location'] = [str(location) for location in data['Location']]
for spot in data['Location']:
    if 'Keelung' in spot or 'Taipei' in spot or 'Taoyuan' in spot or 'Hsinchu' in spot or 'Yilan' in spot:
        area.append('Northern') 
    elif 'Miaoli' in spot or 'Taichung' in spot or 'Changhua' in spot or 'Nantou' in spot or 'Yunlin' in spot:
        area.append('Central')  
    elif 'Chiayi' in spot or 'Tainan' in spot or 'Kaohsiung' in spot or 'Pingtung' in spot:
        area.append('Southern') 
    else:
        area.append('Eastern')
    index += 1
data.insert(0,'Area',area)

# 刪除英文名稱欄位
data = data.drop('Scenic Spots',axis=1)

# 取2023年之1-11月平均，替代2023年12月的值
# data.loc[:,'23Dec'] = (data.loc[:,'23Jan':'23Nov'].sum(axis=1)/11).astype(int)  # 橫向列加總
  
# 計算(2019-2022年)各景點的遊客人次加總
for i in range(5):
    data.loc[:,'%dTotal' % (i+19)] = data.iloc[:,12*i+4:12*i+16].sum(axis=1)    # 橫向列加總

# 計算各景點近五年的遊客人次加總
data.loc[:,'Total'] = data.loc[:,'19Total':'23Total'].sum(axis=1).astype(int)   # 橫向列加總

# 計算年度成長率(取小數點後4位)
for j in range(19,23,1):
    data.loc[:,'%dRate' % (j+1)] = ((data.loc[:,'%dTotal' % (j+1)]-data.loc[:,'%dTotal' % j])/data.loc[:,'%dTotal' % j]).round(4)
data.loc[:,'20Rate':'23Rate'] = (data.loc[:,'20Rate':'23Rate'] * 100).round(4)

# 算疫情前後的成長率(去除2021/5-8月，前後分兩半)
data.loc[:,'before'] = data.loc[:,['21Jan','21Feb','21Mar','21Apr','19Total','20Total']].sum(axis=1)
data.loc[:,'after'] = data.loc[:,['21Sep','21Oct','21Nov','21Dec','22Total','23Total']].sum(axis=1)
data.loc[:,'rate'] = (data.loc[:,'after']-data.loc[:,'before'])/data.loc[:,'before']

# 將欄位名稱加上底線
new_columns = []
for k in data.columns:
    new_columns.append('_'+ k)
data.columns = new_columns

data.info() #--> 看欄位新增後之資料

# 儲存新的csv
data.to_csv('finaldata.csv',index=0)

print('資料預處理完成')
#%%
# 資料庫處理(存取與查詢)
data = pd.read_csv(r'c:\Users\user\Desktop\專題\finaldata.csv',header=0,nrows=286,encoding='utf-8',thousands=',')

try:
    # 連接資料庫
    conn = sqlite3.connect('project.db')    
    print('資料庫連接成功')
    
    # 將資料匯入資料庫
    data.to_sql('finaldata',conn,if_exists='replace',index=False)
    conn.commit()
    print('資料匯入資料庫成功')
    
    # 取各年度各縣市景點之遊客人次與成長率
    all_spots_rate = pd.read_sql('''SELECT "_Area","_Location","_Class","_Name","_19Total","_20Total","_21Total","_22Total","_23Total","_Total","_20Rate","_21Rate","_22Rate","_23Rate" FROM finaldata''', conn) 
    all_spots_total = pd.read_sql('''SELECT "_Area","_Location","_Class","_Name","_19Total","_20Total","_21Total","_22Total","_23Total","_Total" FROM finaldata''', conn)
        
    # 新增一欄值為1
    all_spots_total.insert(0,'num',1)

except Exception as e:
    print('資料庫連接失敗：',e)
    
finally:
    print('資料庫連接結束，資料庫作業完成')

#%%
# 資料分析

# 近五年遊客人次TOP10景點
top10 = all_spots_total.sort_values('_Total',ascending=False).head(10)
top10 = top10.sort_values('_Total')
top10.loc[:,'_19Total':] = top10.loc[:,'_19Total':]/1e6     # 將人次以百萬計

# 近五年遊客人次倒數10景點
low10 = all_spots_total.sort_values('_Total').head(10)
low10.loc[:,'_19Total':] = low10.loc[:,'_19Total':]/1000    # 將人次以千計

# 自訂函式-->用以計算各縣市及區域的人次加總，並計算年度成長率
def groupby_spots(groupby):
    data = all_spots_total.groupby(groupby).sum()
    for col in data.loc[:,['_19Total','_20Total','_21Total','_22Total','_23Total','_Total']]:
        data.loc[:,col] = data.loc[:,col] / data.loc[:,'num']
    for year in range(19,23,1):
        data.loc[:,'_%dRate' % (year+1)] = ((data.loc[:,'_%dTotal' % (year+1)]-data.loc[:,'_%dTotal' % year])/data.loc[:,'_%dTotal' % year]).round(4)
    data.loc[:,'_20Rate':'_23Rate'] = (data.loc[:,'_20Rate':'_23Rate'] * 100).round(4)
    data.loc[:,'_19Total':'_Total'] = data.loc[:,'_19Total':'_Total']/1e6 # 將人次以百萬計
    data = data.reset_index(drop = False)  # 重設index(舊有index為縣市名，不能刪除)
    return data
# 各縣市
country_data = groupby_spots('_Location')
country_data = country_data.drop(['_Class','_Name','_Area'],axis=1) # 刪除_Class、_Name、_Area欄位
def year_top5(column):
    a = data.sort_values(column,ascending=False).head(5)
    return a
top_19 = year_top5('_19Total')
top_20 = year_top5('_20Total')
top_21 = year_top5('_21Total')
top_22 = year_top5('_22Total')
top_23 = year_top5('_23Total')
top_total = year_top5('_Total')
# 各區域
area_data = groupby_spots('_Area')
area_data = area_data.drop(['_Class','_Name','_Location'],axis=1)  # 刪除_Class、_Name、_Location欄位

# 計算各區域景點近五年累積遊客量之前五名
def area_spots(area,year):
    filter1 = all_spots_rate.loc[:,'_Area'] == area
    data = all_spots_rate[filter1]
    top5 = data.sort_values(year,ascending=False).head(5)
    top5.loc[:,'_19Total':'_Total'] = top5.loc[:,'_19Total':'_Total']/1e6  # 將人次以百萬計
    top5 = top5.reset_index(drop = True) # 重設index(舊有的index刪除)
    return top5
northern_top5 = area_spots('Northern','_Total')
central_top5 = area_spots('Central','_Total')
southern_top5 = area_spots('Southern','_Total')
eastern_top5 = area_spots('Eastern','_Total')

# 計算各區域個年度前五名之遊客量
northern_19 = area_spots('Northern','_19Total')
northern_20 = area_spots('Northern','_20Total')
northern_21 = area_spots('Northern','_21Total')
northern_22 = area_spots('Northern','_22Total')
northern_23 = area_spots('Northern','_23Total')
central_19 = area_spots('Central','_19Total')
central_20 = area_spots('Central','_20Total')
central_21 = area_spots('Central','_21Total')
central_22 = area_spots('Central','_22Total')
central_23 = area_spots('Central','_23Total')
southern_19 = area_spots('Southern','_19Total')
southern_20 = area_spots('Southern','_20Total')
southern_21 = area_spots('Southern','_21Total')
southern_22 = area_spots('Southern','_22Total')
southern_23 = area_spots('Southern','_23Total')
eastern_19 = area_spots('Eastern','_19Total')
eastern_20 = area_spots('Eastern','_20Total')
eastern_21 = area_spots('Eastern','_21Total')
eastern_22 = area_spots('Eastern','_22Total')
eastern_23 = area_spots('Eastern','_23Total')

# 疫情前候變化率前五名
rate_top5 = data.sort_values('_rate',ascending=False).head(5)
after_tpo5 = data.sort_values('_after',ascending=False).head(5)
before_tpo5 = data.sort_values('_before',ascending=False).head(5)

print('資料分析完成')
#%%
# 視覺化

#設定字體、大小與負號
plt.rcParams['font.family'] = 'Microsoft JhengHei'
plt.rcParams['font.size'] = 20
plt.rcParams['axes.unicode_minus'] = False

# 近五年全台景點TOP10與LOW10遊客人次
def barh(table,xlabel,fig_name):
    x = [i for i in table['_Name']]
    plt.figure(figsize=(18,8))
    plt.barh(x,table.loc[:,'_19Total'],height=0.5,color='#014B79',label='2019')
    plt.barh(x,table.loc[:,'_20Total'],height=0.5,left=table.loc[:,'_19Total'],color='#45B7C3',label='2020')
    plt.barh(x,table.loc[:,'_21Total'],height=0.5,left=table.loc[:,'_19Total']+table.loc[:,'_20Total'],color='#FFAD49',label='2021')
    plt.barh(x,table.loc[:,'_22Total'],height=0.5,left=table.loc[:,'_19Total']+table.loc[:,'_20Total']+table.loc[:,'_21Total'],color='#8C63B1',label='2022')
    plt.barh(x,table.loc[:,'_23Total'],height=0.5,left=table.loc[:,'_19Total']+table.loc[:,'_20Total']+table.loc[:,'_21Total']+table.loc[:,'_22Total'],color='#FF6F61',label='2023')
    plt.xlabel(xlabel,labelpad=10,fontsize=22)
    plt.legend(loc='lower right')
    plt.savefig(fig_name,bbox_inches = 'tight')
    plt.show()
barh(top10,'人次(單位：百萬)','TOP10遊客人數')
barh(low10,'人次(單位：千)','LOW10遊客人數')

# 各區域之遊客人次及年度成長率
x = [str(i) for i in range(2019,2024,1)]
plt.figure(figsize=(15,8))
bar1 = plt.bar(x,area_data.loc[2,'_19Total':'_23Total'],width=0.5,color='#014B79',label='北')
bar2 = plt.bar(x,area_data.loc[0,'_19Total':'_23Total'],width=0.5,bottom=area_data.loc[2,'_19Total':'_23Total'],color='#45B7C3',label='中')
bar3 = plt.bar(x,area_data.loc[3,'_19Total':'_23Total'],width=0.5,bottom=area_data.loc[2,'_19Total':'_23Total']+area_data.loc[0,'_19Total':'_23Total'],color='#FFAD49',label='南')
bar4 = plt.bar(x,area_data.loc[1,'_19Total':'_23Total'],width=0.5,bottom=area_data.loc[2,'_19Total':'_23Total']+area_data.loc[0,'_19Total':'_23Total']+area_data.loc[3,'_19Total':'_23Total'],color='#8C63B1',label='東')
plt.xlabel('年份',fontsize=22)
plt.ylabel('人\n次\n',rotation=0,labelpad=30,fontsize=22,y=0.4)
legend_order = [bar4, bar3, bar2, bar1]  # 圖例順序調整為 "東"、"南"、"中"、"北"
plt.legend(handles=legend_order,loc='upper right', bbox_to_anchor=(1.13, 1.02))
plt.text(0.91, 0.62, '單位：百萬',fontsize=16,transform=plt.gcf().transFigure)
plt.savefig('各區域遊客人數',bbox_inches = 'tight')
plt.show()

x = [str(i) for i in range(2020,2024,1)]
plt.figure(figsize=(15,8))
plot1 = plt.plot(x,area_data.loc[2,'_20Rate':'_23Rate'],'-',marker='o',color='#014B79',label='北')
plot2 = plt.plot(x,area_data.loc[0,'_20Rate':'_23Rate'],'-',marker='o',color='#45B7C3',label='中')
plot3 = plt.plot(x,area_data.loc[3,'_20Rate':'_23Rate'],'-',marker='o',color='#FFAD49',label='南')
plot4 = plt.plot(x,area_data.loc[1,'_20Rate':'_23Rate'],'-',marker='o',color='#8C63B1',label='東')
plt.axhline(y=0, color='#B4B4B4', linestyle='--', label='0%')
plt.xlabel('年份',fontsize=22)
plt.ylabel('成\n長\n率\n%',rotation=0,labelpad=30,fontsize=22,y=0.4)
plt.legend()
plt.savefig('各區域成長率',bbox_inches = 'tight')
plt.show()

# 中南東近五年TOP5景點遊客人次與遊客人次
def fig(table,label,fig_name):
    plt.figure(figsize=(20,24))
    plt.subplot(2,1,1)
    x = [i for i in table['_Name']]
    bar1 = plt.bar(x,table.loc[:,'_19Total'],width=0.5,color='#014B79',label='2019')
    bar2 = plt.bar(x,table.loc[:,'_20Total'],width=0.5,bottom=table.loc[:,'_19Total'],color='#45B7C3',label='2020')
    bar3 = plt.bar(x,table.loc[:,'_21Total'],width=0.5,bottom=table.loc[:,'_19Total']+table.loc[:,'_20Total'],color='#FFAD49',label='2021')
    bar4 = plt.bar(x,table.loc[:,'_22Total'],width=0.5,bottom=table.loc[:,'_19Total']+table.loc[:,'_20Total']+table.loc[:,'_21Total'],color='#8C63B1',label='2022')
    bar5 = plt.bar(x,table.loc[:,'_23Total'],width=0.5,bottom=table.loc[:,'_19Total']+table.loc[:,'_20Total']+table.loc[:,'_21Total']+table.loc[:,'_22Total'],color='#FF6F61',label='2023')
    plt.xticks(x,label)
    plt.ylabel('人\n次\n',rotation=0,labelpad=30,fontsize=22,y=0.4)  
    legend_order = [bar5, bar4, bar3, bar2, bar1]  # 圖例順序調整為 "2023"、"2022"、"2021"、"2020"、"2019"
    plt.legend(handles=legend_order,loc='upper right')
    plt.text(0.81, 0.77, '單位：百萬',fontsize=16,transform=plt.gcf().transFigure)
    
    plt.subplot(2,1,2)
    x = [str(i) for i in range(2020,2024,1)]
    label1 = [i for i in table['_Name']]
    plt.plot(x,table.loc[0,'_20Rate':'_23Rate'],'-',marker='o',color='#014B79',label=label1[0])
    plt.plot(x,table.loc[1,'_20Rate':'_23Rate'],'-',marker='o',color='#45B7C3',label=label1[1])
    plt.plot(x,table.loc[2,'_20Rate':'_23Rate'],'-',marker='o',color='#FFAD49',label=label1[2])
    plt.plot(x,table.loc[3,'_20Rate':'_23Rate'],'-',marker='o',color='#8C63B1',label=label1[3])
    plt.plot(x,table.loc[4,'_20Rate':'_23Rate'],'-',marker='o',color='#FF6F61',label=label1[4])
    plt.axhline(y=0, color='#B4B4B4', linestyle='--', label='0%')
    plt.xlabel('年份',fontsize=22)
    plt.ylabel('成\n長\n率\n%',rotation=0,labelpad=30,fontsize=22,y=0.4)
    plt.legend()
    plt.savefig(fig_name,bbox_inches = 'tight')
    plt.show()

fig(central_top5,['東豐自行車綠園及\n后豐鐵馬道','北港朝天宮','草悟道','麗寶樂園\n渡假村','日月潭環潭區'],'中部前五名')
fig(southern_top5,['南鯤鯓代天府','旗津風景區','駁二藝術特區','佛光山','大鵬灣遊憩區'],'南部前五名')
fig(eastern_top5,['臺八線沿線\n景觀區','鯉魚潭\n風景特定區','鹿野高臺','池上大坡池\n地區','七星潭風景區'],'東部前五名')

# 北部近五年TOP5景點遊客人次與遊客人次
plt.figure(figsize=(20,24))
plt.subplot(2,1,1)
x = [i for i in northern_top5['_Name']]
bar1 = plt.bar(x,northern_top5.loc[:,'_19Total'],width=0.5,color='#014B79',label='2019')
bar2 = plt.bar(x,northern_top5.loc[:,'_20Total'],width=0.5,bottom=northern_top5.loc[:,'_19Total'],color='#45B7C3',label='2020')
bar3 = plt.bar(x,northern_top5.loc[:,'_21Total'],width=0.5,bottom=northern_top5.loc[:,'_19Total']+northern_top5.loc[:,'_20Total'],color='#FFAD49',label='2021')
bar4 = plt.bar(x,northern_top5.loc[:,'_22Total'],width=0.5,bottom=northern_top5.loc[:,'_19Total']+northern_top5.loc[:,'_20Total']+northern_top5.loc[:,'_21Total'],color='#8C63B1',label='2022')
bar5 = plt.bar(x,northern_top5.loc[:,'_23Total'],width=0.5,bottom=northern_top5.loc[:,'_19Total']+northern_top5.loc[:,'_20Total']+northern_top5.loc[:,'_21Total']+northern_top5.loc[:,'_22Total'],color='#FF6F61',label='2023')
plt.xticks(x,['林口三井\nOutlet','松山文創園區','大溪老城區','淡水金色水岸','國立國父紀念館'])
plt.ylabel('人\n次\n',rotation=0,labelpad=30,fontsize=22,y=0.4)
legend_order = [bar5, bar4, bar3, bar2, bar1]  # 圖例順序調整為 "2023"、"2022"、"2021"、"2020"、"2019"
plt.legend(handles=legend_order,loc='upper right')  
plt.text(0.81, 0.77, '單位：百萬',fontsize=16,transform=plt.gcf().transFigure)

plt.subplot(2,1,2)
x = [str(i) for i in range(2020,2024,1)]
label1 = [i for i in northern_top5['_Name']]
plt.plot(x,northern_top5.loc[0,'_20Rate':'_23Rate'],'-',marker='o',color='#014B79',label=label1[0])
plt.plot(x,northern_top5.loc[1,'_20Rate':'_23Rate'],'-',marker='o',color='#45B7C3',label=label1[1])
plt.plot(x,northern_top5.loc[2,'_20Rate':'_23Rate'],'-',marker='o',color='#FFAD49',label=label1[2])
plt.plot(x,northern_top5.loc[3,'_20Rate':'_23Rate'],'-',marker='o',color='#8C63B1',label=label1[3])
plt.plot(x,northern_top5.loc[4,'_20Rate':'_23Rate'],'-',marker='o',color='#FF6F61',label=label1[4])
plt.axhline(y=0, color='#B4B4B4', linestyle='--', label='0%')
plt.xlabel('年份',fontsize=22)
plt.ylabel('成\n長\n率\n%',rotation=0,labelpad=30,fontsize=22,y=0.4)
plt.ylim(-60, 100)
plt.legend()
plt.savefig('北部前五名',bbox_inches = 'tight')
plt.show()


print('資料視覺化完成')
