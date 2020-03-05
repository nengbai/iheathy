#!/usr/local/bin/python3
# coding= gb18030
#source http://www.cnblogs.com/txw1958/

import os, io, sys, re, time, json, base64
import webbrowser, urllib.request


period_All_List = [
                    "min",      #��ʱ��
                    "daily",    #��K��
                    "weekly",   #��K��
                    "monthly"   #��K��
                  ]
period_min = period_All_List[0]
period_daily = period_All_List[1]

ChinaStockIndexList = [
    "000001", # sh000001 ��ָ֤��
    "399001", # sz399001 ��֤��ָ
    "000300", # sh000300 ����300
    "399005", # sz399005 ��С��ָ
    "399006", # sz399006 ��ҵ��ָ
    "000003",  # sh000003 B��ָ��
]
ChinaStockIndividualList = [
    "000063", #  ����ͨѶ
    "600036", #  ��������
]

WorldStockIndexList = [
    {'code':"000001",'yahoo':"000001.SS",'name':{'chinese':"�й���ָ֤��", 'english':"CHINA SHANGHAI COMPOSITE INDEX"}},
    {'code':"399001",'yahoo':"399001.SZ",'name':{'chinese':"�й���֤��ָ", 'english':"SZSE COMPONENT INDEX"}},
    {'code':"DJI",'yahoo':"DJI",'name':{'chinese':"��������˹��ҵƽ��ָ��", 'english':"Dow Jones Industrial Average"}},
    {'code':"IXIC",'yahoo':"IXIC",'name':{'chinese':"������˹����ۺ�ָ��", 'english':"NASDAQ Composite"},},
    {'code':"GSPC",'yahoo':"GSPC",'name':{'chinese':"������׼�ն�500ָ��", 'english':"S&P 500"}},
    {'code':"N225",'yahoo':"N225",'name':{'chinese':"�ձ��վ�225ָ��", 'english':"NIKKEI 225"}},
    {'code':"TWII",'yahoo':"TWII",'name':{'chinese':"̨��̨����Ȩָ��", 'english':"TSEC weighted index"}},
    {'code':"HSI",'yahoo':"HSI",'name':{'chinese':"��ۺ���ָ��", 'english':"HANG SENG INDEX"}},
    {'code':"FCHI",'yahoo':"FCHI",'name':{'chinese':"����CAC40ָ��", 'english':"CAC 40"}},
    {'code':"FTSE",'yahoo':"FTSE",'name':{'chinese':"Ӣ����ʱ100ָ��", 'english':"FTSE 100"}},
    {'code':"GDAXI",'yahoo':"GDAXI",'name':{'chinese':"�¹������˸�DAXָ��", 'english':"DAX"}}
]
WorldStockIndexList_SP500 =  WorldStockIndexList[7]

#���ڹ�Ʊ���ݣ�ָ��
def getChinaStockIndexInfo(stockCode, period):
    try:
        exchange = "sz" if (int(stockCode) // 100000 == 3) else "sh"
        #http://hq.sinajs.cn/list=s_sh000001
        dataUrl = "http://hq.sinajs.cn/list=s_" + exchange + stockCode
        stdout = urllib.request.urlopen(dataUrl)
        stdoutInfo = stdout.read().decode('gb18030')
        tempData = re.search('''(")(.+)(")''', stdoutInfo).group(2)
        stockInfo = tempData.split(",")
        #stockCode = stockCode,
        stockName   = stockInfo[0]
        stockEnd    = stockInfo[1]  #��ǰ�ۣ�15���Ϊ���̼�
        stockZD     = stockInfo[2]  #�ǵ�
        stockLastEnd= str(float(stockEnd) - float(stockZD)) #���̼�
        stockFD     = stockInfo[3]  #����
        stockZS     = stockInfo[4]  #����
        stockZS_W   = str(int(stockZS) / 100)
        stockJE     = stockInfo[5]  #���
        stockJE_Y   = str(int(stockJE) / 10000)
        content = "#" + stockName + "#" + "(" + str(stockCode) + ")" + " ���̣�" \
          + stockEnd + "���ǵ���" + stockZD + "�����ȣ�" + stockFD + "%" \
          + "�����֣�" + stockZS_W + "��" + "����" + stockJE_Y + "��" + "  "

        imgPath = "http://image.sinajs.cn/newchart/" + period + "/n/" + exchange + str(stockCode) + ".gif"
        twitter = {'message': content, 'image': imgPath}

    except Exception as e:
        print(">>>>>> Exception: " + str(e))
    else:
        return twitter
    finally:
        None

#���ڹ�Ʊ���ݣ�����
def getChinaStockIndividualInfo(stockCode, period):
    try:
        exchange = "sh" if (int(stockCode) // 100000 == 6) else "sz"
        dataUrl = "http://hq.sinajs.cn/list=" + exchange + stockCode
        stdout = urllib.request.urlopen(dataUrl)
        stdoutInfo = stdout.read().decode('gb2312')
        tempData = re.search('''(")(.+)(")''', stdoutInfo).group(2)
        stockInfo = tempData.split(",")
        #stockCode = stockCode,
        stockName   = stockInfo[0]  #����
        stockStart  = stockInfo[1]  #����
        stockLastEnd= stockInfo[2]  #������
        stockCur    = stockInfo[3]  #��ǰ
        stockMax    = stockInfo[4]  #���
        stockMin    = stockInfo[5]  #���
        stockUp     = round(float(stockCur) - float(stockLastEnd), 2)
        stockRange  = round(float(stockUp) / float(stockLastEnd), 4) * 100
        stockVolume = round(float(stockInfo[8]) / (100 * 10000), 2)
        stockMoney  = round(float(stockInfo[9]) / (100000000), 2)
        stockTime   = stockInfo[31]

        content = "#" + stockName + "#(" + stockCode + ")" + " ����:" + stockStart \
        + ",����:" + stockCur + ",���:" + stockMax + ",���:" + stockMin \
        + ",�ǵ�:" + str(stockUp) + ",����:" + str(stockRange) + "%" \
        + ",����:" + str(stockVolume) + "��" + ",���:" + str(stockMoney) \
        + "��" + ",����ʱ��:" + stockTime + "  "

        imgUrl = "http://image.sinajs.cn/newchart/" + period + "/n/" + exchange + str(stockCode) + ".gif"
        twitter = {'message': content, 'image': imgUrl}

    except Exception as e:
        print(">>>>>> Exception: " + str(e))
    else:
        return twitter
    finally:
        None

#ȫ���Ʊָ��
def getWorldStockIndexInfo(stockDict):
    try:
        #http://download.finance.yahoo.com/d/quotes.csv?s=^IXIC&f=sl1c1p2l
        yahooCode = stockDict['yahoo']
        dataUrl = "http://download.finance.yahoo.com/d/quotes.csv?s=" + yahooCode + "&f=sl1c1p2l"

        stdout = urllib.request.urlopen(dataUrl)
        stdoutInfo = stdout.read().decode('gb2312')
        tempData = stdoutInfo.replace('"', '')
        stockInfo = tempData.split(",")
        stockNameCn = stockDict['name']['chinese']
        stockNameEn = stockDict['name']['english']
        stockCode   = stockDict['code']
        stockEnd    = stockInfo[1]  #��ǰ�ۣ�5���Ϊ���̼�
        stockZD     = stockInfo[2]  #�ǵ�
        stockLastEnd= str(float(stockEnd) - float(stockZD)) #���̼�
        stockFD     = stockInfo[3]  #����
        percent     = float(stockFD.replace("%", ""))
        matchResult = re.search("([\w?\s?:]*)(\-)", stockInfo[4])  #���ں�����ֵ
        stockDate   = matchResult.group(1)

        content = "#" + stockNameCn + "# " + stockNameEn + "(" + stockCode + ")" \
          + " ��ǰ��" + stockEnd + ", �ǵ���" + stockZD + ", ���ȣ�" + stockFD \
          + ", �����ʱ�䣺" + stockDate

        twitter = content

    except Exception as err:
        print(">>>>>> Exception: " + yahooCode + " " + str(err))
    else:
        return twitter
    finally:
        None

def test_china_index_data():
    for stockCode in ChinaStockIndexList:
        twitter = getChinaStockIndexInfo(stockCode, period_daily)
        print(twitter['message'] + twitter['image'])

def test_china_individual_data():
    for stockCode in ChinaStockIndividualList:
        twitter = getChinaStockIndividualInfo(stockCode, period_min)
        print(twitter['message'] + twitter['image'])

def test_global_index_data():
    for stockDict in WorldStockIndexList:
        print(getWorldStockIndexInfo(stockDict))


def main():
    "main function"
    print(base64.b64decode(b'Q29weXJpZ2h0IChjKSAyMDEyIERvdWN1YmUgSW5jLiBBbGwgcmlnaHRzIHJlc2VydmVkLg==').decode())
    test_china_index_data()
    test_china_individual_data()
    test_global_index_data()

if __name__ == '__main__':
    main()