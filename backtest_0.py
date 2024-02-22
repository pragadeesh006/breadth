import yfinance as yf
from datetime import datetime
import pandas as pd
import quantstats as qs
import talib as ta
import matplotlib.pyplot as plt 
import numpy as np
import concurrent.futures
from jugaad_data.nse import NSELive
import threading
import scipy.stats as stats
n = NSELive()

def fetchhisdata(x,period,interval,t1,t2,mode):
    # CREATE TICKER INSTANCE FOR AMAZON
    stock = yf.Ticker(x)

    # GET TODAYS DATE AND CONVERT IT TO A STRING WITH YYYY-MM-DD FORMAT (YFINANCE EXPECTS THAT FORMAT)
    # pip install yahooquery for yf's alternative
    
    #end_date = datetime.now().strftime('%Y-%m-%d')
    stock_history = stock.history(period=period,interval=interval)
    if (mode == 0):
        stock_close = ((stock_history['Close'].iloc[t2] - stock_history['Close'].iloc[t1]) /
                      stock_history['Close'].iloc[t1])*100
        stock_close_prevday = ((stock_history['Close'].iloc[t1] - stock_history['Close'].iloc[t1-1]) /
                      stock_history['Close'].iloc[t1-1])*100
        #print(stock_history['Close'].iloc[t2],stock_history['Close'].iloc[t1],x,t2,t1
    elif (mode == 1):
        stock_close = ((stock_history['Close'][t1] - stock_history['Close'][t2]) /
                  stock_history['Close'][t2])*100
    open_date = stock_history.index.strftime('%Y-%m-%d')[t1]
    cl_date = stock_history.index.strftime('%Y-%m-%d')[t2]
    return stock_close, open_date, cl_date, stock_close_prevday

close=[]
def topgainerloser():
    count=0
    """returns a sorted(descending) dataframe relative to returns"""
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbols = index['nifty100']
    for symbol in symbols:
            close.append(fetchhisdata((symbol+'.NS'),"2d","15m",-25,-26))
            #print(symbol)
    topgl_dict={'symbol':symbols,'return':close}
    topgl_df = pd.DataFrame(topgl_dict)
    topgl_df = topgl_df.sort_values(by=['return'], ascending=False)
    #topgl_df.to_csv('/Users/punisher/Desktop/x/cs/stonks1/excel1.csv',mode='a')
    for number in topgl_df["return"]:
        if (number > 0.00000):
            count=count+1
    print(topgl_df,'\n',count)
    return topgl_df

def midday_endday_diff(topgl_df,close):
    """to find the difference between midday, endday of a return distribution""" 
    close1=[]
    count1=0
    symbols1=topgl_df['symbol']
    for symbol in symbols1:
        close1.append(fetchhisdata((symbol+'.NS'),"5d","1d",-1,-2))
    topgl_df['cl_return'] = close1
    topgl_df['difference'] = topgl_df['cl_return'] - topgl_df['return']
    #topgl_df = topgl_df.sort_values(by=['cl_return'], ascending=False)
    #topgl_df.to_csv('/Users/punisher/Desktop/x/cs/stonks1/excel1.csv',mode='a')
    for number in topgl_df["cl_return"]:
        if (number > 0.00000):
            count1=count1+1
    print(topgl_df.tail(10),'\n',sum(topgl_df["difference"].tail(10)),'\n',count1)
    
def midday_endday_diff_unified(t1,t2,t3,t4):
    close0=[]
    count=0
    close1=[]
    count1=0
    """returns a sorted(descending) dataframe relative to returns"""
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbols = index['Symbol']
    for symbol in symbols:
            close0.append(fetchhisdata((symbol+'.NS'),"40d","1d",t1,t2,0))
            #print(symbol)
    topgl_dict={'symbol':symbols,'return':close0}
    topgl_df = pd.DataFrame(topgl_dict)
    topgl_df = topgl_df.sort_values(by=['return'], ascending=False)
    #topgl_df.to_csv('/Users/punisher/Desktop/x/cs/stonks1/excel1.csv',mode='a')
    for number in topgl_df["return"]:
        if (number > 0.00000):
            count=count+1
    print(topgl_df,'\n',count)
    symbols1=topgl_df['symbol']
    for symbol in symbols1:
        close1.append(fetchhisdata((symbol+'.NS'),"40d","1d",t3,t4,0))
    topgl_df['cl_return'] = close1
    topgl_df['difference'] = topgl_df['cl_return'] - topgl_df['return']
    topgl_df = topgl_df.sort_values(by=['cl_return'], ascending=False)
    topgl_df.to_csv('/Users/punisher/Desktop/x/cs/stonks1/excel1.csv',mode='a')
    for number in topgl_df["cl_return"]:
        if (number > 0.00000):
            count1=count1+1
    value=30
    print(topgl_df.head(value),'\n',sum(topgl_df["difference"].head(value)),'\n',topgl_df.tail(value),
          '\n',sum(topgl_df["difference"].tail(value)),'\n',count1)
    
def midday_endday_diff_multipledays(lookback_period):
    x=-5
    y=-1
    for _ in range (lookback_period):
        midday_endday_diff_unified(x, x-5, y, y-4)
        x+=-5
        y+=-5
        
def volume(p,interval1,interval2,choice):
    list1=[]
    list2=[]
    list3=[]
    list4=[]
    list5=[]
    list6=[]
    list7=[]
    i=0
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbols = index['Symbol']
    for symbol in symbols:
        i=i+1
        stock = yf.Ticker(symbol+'.NS')
        stock_history = stock.history(period=p,interval="1d")
        stock_volume = stock_history['Volume'].iloc[interval1]
        avg_vol=stock_history['Volume'][interval1-20:interval1].mean()
        vol_chg=stock_volume/avg_vol
        #list1.append(stock_volume)
        list2.append(vol_chg)
        a,b,c,d = fetchhisdata(symbol+'.NS', "100d", "1d", interval1, interval2, 0)
        list4.append(a)
        list5.append(b)
        list6.append(c)
        list7.append(d)
    voldict={'symbol':symbols,"vol_chg":list2,"return":list4,"clday_rtn":list7,
             "o_date":list5,"c_date":list6}
    dataframe1=pd.DataFrame(voldict)
    dataframe2=dataframe1.sort_values(by=['vol_chg'], ascending=False)
    df2=dataframe2.head(20)
    if (choice == 1):
        print(df2)
    elif (choice == 0):
        print(df2[df2['return'] < 0.0000])
    x_values = np.arange(0, 20)
    plt.figure(figsize=(10, 6))
    #graph : plots the returns of all stocks head(20) of top volume ratio
    plt.plot(x_values, df2['return'], label='Return')
    plt.title('Line Plot of Return')
    plt.xlabel('val')
    plt.ylabel('Return')
    plt.legend()
    plt.xticks(x_values)
    plt.axhline(y=0, color='red', linestyle='--', label='Y=0')
    plt.show()
    
def volume_loop(lookback_prd,z):
    j=-10
    k=-1
    #parameter z lets you decide if you want to print head(20)--z=1 or only the losers of head(20)--z=0
    #lookback_prd lets you to decide how many days you want the backtester to make calculations for
    for _ in range(lookback_prd):
        volume("100d",j,k,z)
        j+=-1
        k+=-1

def breadth(p,mode,h,j,list_pos):
    monthly_gainers = []
    unified_gainers_list = []
    i=0
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbols = index['Symbol']
    list_intervals=[[-1+h,-2+h,"todays stat>4%",4.00000],[-1+h,-5+h,"5days stat>10%",10.0000],
                        [-1+h,-10+h,"10days stat>15%",15.00000],[-1+h,-30+h,"1month stat>25%",25.00000],
                        [-1+h,-30+h,"1month stat>50%",50.00000]]
    for i in range(5):
        count=0
        gainer_symbol=[]
        gainers=[]
        dict1={}
        df1=[]            
        for symbol in symbols:
            stock = yf.Ticker(symbol+'.NS')
            try:
                stock_history = stock.history(period=p,interval="1d")
                return_interval=((stock_history['Close'].iloc[list_intervals[i][0]] -
                                  stock_history['Close'].iloc[list_intervals[i][1]]) /
                              stock_history['Close'].iloc[list_intervals[i][1]])*100
            #print(return_interval,symbol)
            except:
                return_interval=0
                
            if (return_interval > list_intervals[i][3]):
                count+=1
                unified_gainers_list.append(symbol)
                if(mode==1):
                    gainer_symbol.append(symbol)
                    gainers.append(return_interval)
                    dict1={"symbol":gainer_symbol,"gains":gainers}
                    df1=pd.DataFrame(dict1)
                if (i==list_pos)and (j==1):
                    monthly_gainers.append(symbol)
                    file=open("txtfile.txt","w")
                    for element in monthly_gainers:
                        file.write(str(element) + '\n')

        if (mode==1):
            print((df1.sort_values(by=['gains'], ascending=False)).head(20))
        print(list_intervals[i][2],"gainers:", count, "losers:", 500-count)
    duplicates = {item for item in unified_gainers_list if unified_gainers_list.count(item) > 4}
    print(duplicates)
    
def timeseries_breadth(t):
    for i in range(0,t):
        print("day", i)
        if (i==0):
            breadth("40d",0,(-1*i),1,4)
            print('\n')
        else:
            breadth("40d",0,(-1*i),0,4)
            print('\n')
        
def plot(t,run_breadth_forlist,list_pos):
    if(run_breadth_forlist == 1):
        breadth("40d",0,0,1,list_pos-1)
    industry_value=[]
    stock_data = {}
    monthly_gainers = []
    with open("txtfile.txt", 'r') as file:
        for line in file:
            element = line.strip()
            monthly_gainers.append(element)
        
    for symbol in monthly_gainers:
        try:
            stock = yf.Ticker(symbol+'.NS')
            stock_history = stock.history(period=t, interval="1d")
            q = n.stock_quote(symbol)
            stock_data[symbol] = stock_history['Close']
            
            # Check if 'info' is in the response
            if 'info' in q:
                industry_value.append(q['info'].get('industry', 'N/A'))
            else:
                industry_value.append('N/A')
        except:
            industry_value.append('N/A')

    dict1={'symbol':monthly_gainers,"industry":industry_value}
    df1=pd.DataFrame(dict1)
    print(df1)
     
    plt.figure(figsize=(12,8))

    for symbol, data in stock_data.items():
        plt.plot(data.index, data, label=symbol)
    plt.title('Stock plot of Monthly Gainers')
    plt.xlabel('Date')
    plt.ylabel('closing prices')
    #plt.xticks(data.index)
    plt.show()
    duplicates = {item for item in industry_value if industry_value.count(item) > 2}
    print(duplicates)

def consistent_return(t,Return):
    symbol_df=[]
    count=[]
    dict_plot={}
    plot_cnt=0
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbol = index['Symbol']
    for symbols in symbol:
        plot_cnt+=1
        positive_returns_count=0
        stock = yf.Ticker(symbols+'.NS')
        stock_history = stock.history(period=t, interval="1d")
        stock_return=(stock_history['Close'].pct_change())*100
        positive_returns_count = (stock_return >= (Return*1.000000)).sum() #------>should be a numpy array
        symbol_df.append(symbols)
        count.append(positive_returns_count)
        if (plot_cnt <=20):
            dict_plot[symbols] = stock_return
    dict_df={'symbol':symbol_df,'+ve return count':count}
    print(len(symbol_df))
    df=pd.DataFrame(dict_df)
    df=df.sort_values(['+ve return count'], ascending=False)
    print(df.head(20))
    for symbol,close in dict_plot.items():
        plt.plot(close.index, close, label=symbol)
    plt.title('rtn of consistent performers')
    plt.xlabel('date')
    plt.ylabel('closing prices')
    plt.show()
    
def stddev_data(symbol):
    tar_rtn = 0.0
    try:
        stock = yf.Ticker(symbol + '.NS')
        stock_history = stock.history(period="1y", interval="1d")
        stock_return = (stock_history['Close'].pct_change())*100
        stock_return = stock_return[stock_return != stock_return.max()]
        stock_return = stock_return[stock_return != stock_return.min()]
        
        mean_annual_return = stock_return.mean()
        risk_annual = stock_return.std()
        mean = (1 + mean_annual_return / 100)**21 - 1
        risk = risk_annual * np.sqrt(21)
        
        z_score = (tar_rtn - mean) / risk
        probability = 1 - stats.norm.cdf(z_score)
        #probability = stats.norm.cdf(z_score)
        open_date = stock_history['Close'].index[0].year
    except:
        risk = 0
        stock_return = 0
        stock_history = 0
        open_date = 0
    return symbol, risk, mean, probability, open_date

def risk_measure():
    index = pd.read_csv("/Users/punisher/Desktop/x/cs/stonks1/index.csv")
    symbol = index['Symbol'].tolist()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(stddev_data, symbol))

    symbol, risk, mean, probability, open_date = zip(*results)

    dict_df = {'symbol': symbol, 'risk': risk, 'mean': mean, 'p:R>0': probability, 'open_date': open_date}
    df = pd.DataFrame(dict_df)
    # df = df.sort_values(['p:R>0'], ascending=False)
    # print(df[df['open_date'] < 2018].head(50))
    # plt.plot(df['symbol'],df['p:R>0'])
    # plt.xticks(rotation=90)
    # plt.show()
    print(df)
    
def returns(symbol, weight):
    stock = yf.Ticker(symbol)
    stock_history = stock.history(start="2024-01-01",end="2024-01-30", interval="1d")
    stock_return = (((stock_history['Close'][-1])-(stock_history['Close'][0]))/(stock_history['Close'][0]))*100
    return stock_return*weight

def backereturn_calculator():
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = list(executor.map(lambda s,j: returns(s,j),symbols,weights))
    print(sum(result))