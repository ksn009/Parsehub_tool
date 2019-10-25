from flask import Flask, request, render_template
import requests
import json
import csv
from datetime import datetime
from io import StringIO
from werkzeug.wrappers import Response
import mysql.connector
import time

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="scrape_parsehub"
)
mycursor = mydb.cursor()

now = datetime.now()
now1="project1_"+str(now)+".csv"
app = Flask(__name__)

api_list=['t5QpzJ5WqUoE','tqfTT-tZebt0','t8yq90SEaHk9','tvOFW19xBdBz','tvct8117-qjo','tKhMgLLqenM3','ttTxMj1hYnoZ','t0F5BD-Y96tq',
          'tH91mAar-nHP','twhXaTsgn49Z','tpwc8cPfTSLv','tqxv9bsXmEnM','ttkS859s2wku','ty4AjFWxmLXD','t6svuaX8kXSt','tvmzBHODR8-m',
          'tNLjeYj6XVLE','tTgnG2Be7wih','tv47dCOmiavk','tavQyKTnj42O','tiAgn1aY2jrz','tdMK7tV6_qZq','tG1eo8sNGmYP','tf7o_zyLPoYb',
          'tziC4p8x1BJE','tfShBYRKUpEY','ty_o6U-G6CRT','tb2z_mvTyfEH','t-f4O6OUqNS7','tY4VW1-gceYR']
pt_list_pp =['tY6XObdw2Fuv','tPtLkvOujKPB','tJGhbgdXdqFT','t5vK3Me03t3E','t3A9Up-WDTb-','ttEtS7ztGWaG','t88TDtGrJy2M','tgTZHhUFYgO3',
          'tTqqUs138R2q','tzYZzTtz0zeT','t74cHnXF7wre','tZBcBM8ku0uR','tArewbFLSOab','tx4vUvLJp8DA','t-y_hB_Q79hh','tS4GvqUXc3Kx',
          't5vYOTaWStHg','tGGVTDmixcwx','tTac78QGyhHY','th1iPe_u7Lfh','txj5jf5TRgym','tE8t9Oc2cTtr','tFH9TqwSmU0u','tWT8Lg9Zrc6w',
          'toTTR3Zswa4g','tAtVh1qKX5Xo','tE3bTVsvRgo5','tfNSWA8pTWkh','tWf9xpCVEP2T','tSDjVOLidzXS']
pt_list_bs =['tPhQMxEdN-0q', 'tm46hOvLdtas', 'tL7YYH5s6BRE', 'tKi-1Fi_wgLO', 'tujYvK3OcTYV', 'tiS225Tk4esu', 'tV_79t39t0Sh', 
          'trhqBqCPqJci', 'tdRyfdaq6Xrt', 'ti9BVWh0Kezq', 'tna-5T_DJjC8', 'tyHPMQrJqnPC', 'tyn0sTNJ1jF8', 'tPBob9et8teZ', 
          'tgdcDxKOXiqJ', 'tsj4MZRysrR2', 'tA1xwZQJrTFX', 'tOOwiqW9AQ-5', 'td_CxMT0Qv15', 'tPTHaojmsJrj', 't-ATyYSC-u1d', 
          'tP2TV23hEfNb', 'to6_28JX6KWn', 'tML3vzUWUtTU', 'tPQiP7RYSRfa', 'tPb8MeNbyCyJ', 'triPJciqcjc3', 't9QvFwTfkYWi', 
          'tjBBPkiAbZ0a', 't7tYw8L7d6X8']
pt_list_sc =['txEBr3EbTdJu', 'tcVNzTUt3PoY', 'tGTVVtoaSfp2', 't9bxPFM9eDO9', 'tUZUCBCbXBu7', 'tmmz73PEidgT', 'tmrXerCT46Av', 
          'tH0TS0FkZXWm', 'tAUivSMSEQng', 't4yt6cpTdfyn', 'ts0z-T_UJ53L', 't_Q9dENXchYA', 'tk_1Zhhqg3G4', 'tvutN0nom0JT', 
          'tY-zU5APLk_T', 'tU6KEeuME265', 'twpWXa5KQfuT', 'tvpRRWa2sasN', 'tFj1P20sciRc', 'taAmg4nWZOHT', 'tC11QZT3zZ0D', 
          'tAXxjr9vO1VS', 'tpQisH1SLd6B', 'tZvG_sutbQKD', 'tMHnbTsSL4jk', 'tf1dM_Qst97T', 'tv5OG4E5P_L8', 'thDCQNurTTQt', 
          'tNT4vZuuhNs6', 'thrsUOeLejx2']
pt_list_pl =['tBe8vTtLNtsz','tc7e5MbL4C0T','trnH1KDqO8X_','tQXeOTewtAxn','th1oxdz-USyE','t-2RTFDb3akO','t7cv66Oewnpx','tsyPyJgX40Ej',
          'tDELs5G_L7Yz','tjdMRk9c6uX0','tTnduJ1HC1sF','tYuWGMYmpnGe','tHJxMUX_i8XS','tWvd_oxeTz2t','tVqj57CtGzZh','t5MuG7hFE5GT',
          't80z50WvMRsa','t2vv-7SKe6pb','tdX4cf6bvDka','tVSSe7Q2P66Z','txwapiv7WqRz','tBL8LL3Amj5H','tenzhNpXLCqm','tTGfg7k0FJT8',
          'toTTR3Zswa4g','tkwQ4hQgYtkx','tE_JG_7OfMBJ','t8h6eCJUAzVZ','t1t3pRyJpsVE','tCi8FEq2TVYq']


@app.route('/')
def my_form():
    return render_template('index.html')


@app.route('/run_pl', methods=['POST'])
def run_pl():
    return render_template('run_pl.html')
@app.route('/run_pp', methods=['POST'])
def run_pp():
    return render_template('run_pp.html')
@app.route('/run_bs', methods=['POST'])
def run_bs():
    return render_template('run_bs.html')
@app.route('/run_sc', methods=['POST'])
def run_sc():
    return render_template('run_sc.html')

@app.route('/get_bs', methods=['POST'])
def get_bs():
    drop=[]
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT run_name FROM input_bestseller")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get_bs.html',runt_drop=drop)

@app.route('/get_pp', methods=['POST'])
def get_pp():
    drop=[]
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT run_name FROM input_data_p3")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get_pp.html',runt_drop=drop)

@app.route('/get_pl', methods=['POST'])
def get_pl():
    drop=[]
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT run_name FROM input_data")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get_pl.html',runt_drop=drop)

@app.route('/get_sc', methods=['POST'])
def get_sc():
    drop=[]
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT run_name FROM input_subcat1")
    myresult = mycursor.fetchall()
    for t in myresult:
        drop.append(t[0])
    return render_template('get_sc.html',runt_drop=drop)

@app.route('/run_data_pl', methods=['POST'])
def run_data_pl():
    api=[]
    pt=[]
    v=[]    

    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    for n in range(0,len(api_list)):
        params = {
                "api_key": api_list[n],
                "offset": "0",
                "limit": "20",
                "include_options": "1"
                }
        r = requests.get('https://www.parsehub.com/api/v2/projects', params=params)
        d=json.loads(r.text)
        for i in range(0,int(d['total_projects'])):
            if (d['projects'][i]['last_run'] == None) or (d['projects'][i]['last_run']['status'] == 'complete'):
                ready = True
            else:
                ready = False
                break
        if ready == True:
            api.append(api_list[n])
            pt.append(pt_list_pl[n])
    link= request.form['link_pl']
    link1=link.split()
    link_len=len(link1)
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    max_link = len(api)*199
    un_processing_link_numb=0
    processing_link=[]
    un_processing_link=[]
    links199=[]
    h=0
    run_tok=""
    res=""
    link_temp=""
    j="\",\""
    if link_len != 0:
        if link_len < 199:
            api_sel= 1
            api_r = 0
            count_start=0
            count_max=link_len
            count_max_temp=link_len
            count_start_sql=0
            count_max_sql=link_len
        elif link_len > max_link:
            api_sel=len(api)
            api_r=0
            count_start=0
            count_max=max_link
            count_start_sql=0
            count_max_sql=max_link
            count_max_temp=199
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=199
            count_start_sql = 0
            count_max_sql=199
        if link_len > max_link:
            for n in range(0,max_link):
                processing_link.append(link1[n])
            un_processing_link_numb=link_len-max_link
            for n in range((link_len-un_processing_link_numb),link_len):
                un_processing_link.append(link1[n])
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        if link_len < max_link:
            for ii in range(0,api_sel):
                link_temp199=[]
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) >= 199:
                    count_max_temp = count_max_temp + 199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                h=ii+1
            link_temp199=[]
            v=[]
            if api_r != 0:
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[h],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[h],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[h]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[h],pt[h],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()                    
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
            res="Successfully Completed "+str(h)+'<br>'+" Projects Run Name: "+r_name+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,len(api)):
                link_temp199=[]
                for i in range(count_start,count_max):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) > 199:
                    count_max_temp = count_max_temp+199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(5)
            res="Successfully Completed "+str('30')+" Projects Run Name: "+r_name+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]		
            return res
    else:
        return "Please Enter Vaild Data"
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
@app.route('/run_data_sc', methods=['POST'])
def run_data_sc():
    api=[]
    pt=[]
    v=[]    

    mycursor.execute("CREATE TABLE IF NOT EXISTS input_subcat1 (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    for n in range(0,len(api_list)):
        params = {
                "api_key": api_list[n],
                "offset": "0",
                "limit": "20",
                "include_options": "1"
                }
        r = requests.get('https://www.parsehub.com/api/v2/projects', params=params)
        d=json.loads(r.text)
        for i in range(0,int(d['total_projects'])):
            if (d['projects'][i]['last_run'] == None) or (d['projects'][i]['last_run']['status'] == 'complete'):
                ready = True
            else:
                ready = False
                break
        if ready == True:
            api.append(api_list[n])
            pt.append(pt_list_sc[n])
    link= request.form['link_sc']
    link1=link.split()
    link_len=len(link1)
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    max_link = len(api)*199
    un_processing_link_numb=0
    processing_link=[]
    un_processing_link=[]
    links199=[]
    h=0
    run_tok=""
    res=""
    link_temp=""
    j="\",\""
    if link_len != 0:
        if link_len < 199:
            api_sel= 1
            api_r = 0
            count_start=0
            count_max=link_len
            count_max_temp=link_len
            count_start_sql=0
            count_max_sql=link_len
        elif link_len > max_link:
            api_sel=len(api)
            api_r=0
            count_start=0
            count_max=max_link
            count_start_sql=0
            count_max_sql=max_link
            count_max_temp=199
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=199
            count_start_sql = 0
            count_max_sql=199
        if link_len > max_link:
            for n in range(0,max_link):
                processing_link.append(link1[n])
            un_processing_link_numb=link_len-max_link
            for n in range((link_len-un_processing_link_numb),link_len):
                un_processing_link.append(link1[n])
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        if link_len < max_link:
            for ii in range(0,api_sel):
                link_temp199=[]
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) >= 199:
                    count_max_temp = count_max_temp + 199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_subcat1 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                h=ii+1
            link_temp199=[]
            v=[]
            if api_r != 0:
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[h],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[h],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[h]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_subcat1 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[h],pt[h],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()                    
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
            res="Successfully Completed "+str(h)+'<br>'+" Projects Run Name: "+r_name+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,len(api)):
                link_temp199=[]
                for i in range(count_start,count_max):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) > 199:
                    count_max_temp = count_max_temp+199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_subcat1 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(5)
            res="Successfully Completed "+str('30')+" Projects Run Name: "+r_name+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]		
            return res
    else:
        return "Please Enter Vaild Data"
#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
@app.route('/run_data_bs', methods=['POST'])
def run_data_bs():
    api=[]
    pt=[]
    v=[]    

    mycursor.execute("CREATE TABLE IF NOT EXISTS input_bestseller (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    for n in range(0,len(api_list)):
        params = {
                "api_key": api_list[n],
                "offset": "0",
                "limit": "20",
                "include_options": "1"
                }
        r = requests.get('https://www.parsehub.com/api/v2/projects', params=params)
        d=json.loads(r.text)
        for i in range(0,int(d['total_projects'])):
            if (d['projects'][i]['last_run'] == None) or (d['projects'][i]['last_run']['status'] == 'complete'):
                ready = True
            else:
                ready = False
                break
        if ready == True:
            api.append(api_list[n])
            pt.append(pt_list_bs[n])
    link= request.form['link_bs']
    link1=link.split()
    link_len=len(link1)
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    max_link = len(api)*99
    un_processing_link_numb=0
    processing_link=[]
    un_processing_link=[]
    links99=[]
    h=0    
    run_tok=""
    res=""
    link_temp=""
    j="\",\""
    if link_len != 0:
        if link_len < 99:
            api_sel= 1
            api_r = 0
            count_start=0
            count_max=link_len
            count_max_temp=link_len
            count_start_sql=0
            count_max_sql=link_len
        elif link_len > max_link:
            api_sel=len(api)
            api_r=0
            count_start=0
            count_max=max_link
            count_start_sql=0
            count_max_sql=max_link
            count_max_temp=99
        else:
            api_sel = int(link_len/99)
            if link_len%99 != 0:
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=99
            count_start_sql = 0
            count_max_sql=99
        if link_len > max_link:
            for n in range(0,max_link):
                processing_link.append(link1[n])
            un_processing_link_numb=link_len-max_link
            for n in range((link_len-un_processing_link_numb),link_len):
                un_processing_link.append(link1[n])
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        if link_len < max_link:
            for ii in range(0,api_sel):
                link_temp99=[]
                for i in range(count_start,count_max_temp):
                    link_temp99.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) >= 99:
                    count_max_temp = count_max_temp + 99
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp99)
                links99.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links99[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_bestseller (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 99:
                        count_max_sql=count_max_sql+99
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                h=ii+1
            link_temp99=[]
            v=[]
            if api_r != 0:
                for i in range(count_start,count_max_temp):
                    link_temp99.append(processing_link[i])
                link_temp=j.join(link_temp99)
                links99.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[h],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links99[h],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[h]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_bestseller (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[h],pt[h],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()                    
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
            res="Successfully Completed "+str(h)+'<br>'+" Projects Run Name: "+r_name+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,len(api)):
                link_temp99=[]
                for i in range(count_start,count_max):
                    link_temp99.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) > 99:
                    count_max_temp = count_max_temp+99
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp99)
                links99.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links99[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_bestseller (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 99:
                        count_max_sql=count_max_sql+99
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(1)
            res="Successfully Completed "+str('30')+" Projects Run Name: "+r_name+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]		
            return res
    else:
        return "Please Enter Vaild Data"
#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
@app.route('/run_data_pp', methods=['POST'])
def run_data_pp():
    api=[]
    pt=[]
    v=[]    

    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data_p3 (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    for n in range(0,len(api_list)):
        params = {
                "api_key": api_list[n],
                "offset": "0",
                "limit": "20",
                "include_options": "1"
                }
        r = requests.get('https://www.parsehub.com/api/v2/projects', params=params)
        d=json.loads(r.text)
        for i in range(0,int(d['total_projects'])):
            if (d['projects'][i]['last_run'] == None) or (d['projects'][i]['last_run']['status'] == 'complete'):
                ready = True
            else:
                ready = False
                break
        if ready == True:
            api.append(api_list[n])
            pt.append(pt_list_pp[n])
    link= request.form['link_pp']
    link1=link.split()
    link_len=len(link1)
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    max_link = len(api)*199
    un_processing_link_numb=0
    processing_link=[]
    un_processing_link=[]
    links199=[]
    h=0
    run_tok=""
    res=""
    link_temp=""
    j="\",\""
    if link_len != 0:
        if link_len < 199:
            api_sel= 1
            api_r = 0
            count_start=0
            count_max=link_len
            count_max_temp=link_len
            count_start_sql=0
            count_max_sql=link_len
        elif link_len > max_link:
            api_sel=len(api)
            api_r=0
            count_start=0
            count_max=max_link
            count_start_sql=0
            count_max_sql=max_link
            count_max_temp=199
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=199
            count_start_sql = 0
            count_max_sql=199
        if link_len > max_link:
            for n in range(0,max_link):
                processing_link.append(link1[n])
            un_processing_link_numb=link_len-max_link
            for n in range((link_len-un_processing_link_numb),link_len):
                un_processing_link.append(link1[n])
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        if link_len < max_link:
            for ii in range(0,api_sel):
                link_temp199=[]
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) >= 199:
                    count_max_temp = count_max_temp + 199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data_p3 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                h=ii+1
            link_temp199=[]
            v=[]
            if api_r != 0:
                for i in range(count_start,count_max_temp):
                    link_temp199.append(processing_link[i])
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                    "api_key": api[h],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[h],
                    "send_email": "1"
                    }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[h]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data_p3 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[h],pt[h],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()                    
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
            res="Successfully Completed "+str(h)+'<br>'+" Projects Run Name: "+r_name+'<br><br>'+ 'Run Token For Projects:'+'<br>'+run_tok
            return res
        else:
            for ii in range(0,len(api)):
                link_temp199=[]
                for i in range(count_start,count_max):
                    link_temp199.append(processing_link[i])
                count_start=i+1
                if (link_len-count_start) > 199:
                    count_max_temp = count_max_temp+199
                else:
                    count_max_temp=link_len
                link_temp=j.join(link_temp199)
                links199.append("{\"url\":[\""+link_temp+"\"]}")
                params = {
                        "api_key": api[ii],
                        "start_url": "https://www.amazon.in/","start_template": "main_template",
                        "start_value_override": links199[ii],
                        "send_email": "1"
                        }
                r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt[ii]+"/run", data=params)
                res = res +'<br>'+ r.text
                y=json.loads(r.text)
                if y['run_token'] != '':
                    for s in range(count_start_sql,count_max_sql):
                        sql = "INSERT INTO input_data_p3 (api,pt,link,runt,run_name) VALUES (%s, %s, %s, %s, %s)"
                        v=(api[ii],pt[ii],processing_link[s],y['run_token'],r_name)
                        mycursor.execute(sql, v)
                        mydb.commit()
                    count_start_sql=s+1
                    if (link_len-count_start_sql) > 199:
                        count_max_sql=count_max_sql+199
                    else:
                        count_max_sql=link_len
                    run_tok=run_tok+'<br>'+y['run_token']
                else:
                    return "Error in Running Project"+'<br><br>'+res
                time.sleep(1)
            res="Successfully Completed "+str('30')+" Projects Run Name: "+r_name+'<br><br>'+'Run Token For Projects:'+'<br>'+run_tok+'<br><br>'+"Un Processed Links Due to Over Load <br>" 
            for nn in range(0,un_processing_link_numb):
                res = res + '<br>' + un_processing_link[nn]		
            return res
    else:
        return "Please Enter Vaild Data"
#//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
@app.route('/get_data_pl', methods=['POST'])
def get_data_pl():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_data (id INT AUTO_INCREMENT PRIMARY KEY, product_name VARCHAR(800), product_url VARCHAR(800), runt VARCHAR(25), ip_link VARCHAR(255), run_name VARCHAR(25))")
    v=[]
    p_url=[]
    p_name=[]
    ip_link=[]
    ip_url=[]
    run_token=[]
    run_token_sql=[]
    api1=[]
    r_name = request.form['runt_drop']
    sql = "SELECT DISTINCT runt FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])
    sql = "SELECT DISTINCT link FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        ip_url.append(t[0])
    sql = "SELECT DISTINCT api FROM input_data WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1.append(t[0])
    sql = "SELECT DISTINCT runt FROM scrap_data WHERE run_name LIKE '%"+r_name+"%'"
    adr = (r_name,)
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    for t in range(0,len(api1)):
        params = {
                "api_key": api1[t]
                }
        r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[t], params=params)
        y=json.loads(r.text)
        if y['status'] == 'complete':
            continue
        else:
            return " PLEASE WAIT.... Data Not Ready "
    if len(store_var) == 0:
        for z in range(0,len(api1)):
            params = {"api_key": api1[z],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[z]+'/data', params=params)
            y = json.loads(r.text)
            for n in range (0,len(y['details'])):
                if 'selection1' in (y['details'][n]):
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        if ("name" in y['details'][n]['selection1'][nn]) and ("url" in y['details'][n]['selection1'][nn]):
                            p_name.append(y['details'][n]['selection1'][nn]['name'])
                            p_url.append(y['details'][n]['selection1'][nn]['url'])
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        ip_link.append(ip_url[n])
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        run_token_sql.append(run_token[z])
                else:
                    continue
        p_len=len(p_name)        
        for i in range(0,p_len):
            v.append((p_name[i],p_url[i],run_token_sql[i],ip_link[i],r_name))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_name', 'Product_URL','Input Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[3]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        sql = "INSERT INTO scrap_data (product_name,product_url,runt,ip_link,run_name) VALUES (%s, %s, %s, %s, %s)"
        mycursor.executemany(sql, v)
        mydb.commit()
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
    else:
        sql = "SELECT product_name, product_url, ip_link FROM scrap_data WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_name', 'Product_URL','Input Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[2]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@app.route('/get_data_sc', methods=['POST'])
def get_data_sc():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_subcat1 (id INT AUTO_INCREMENT PRIMARY KEY, product_name VARCHAR(800), product_url VARCHAR(800), product_page VARCHAR(800), run_name VARCHAR(25))")
    v=[]
    p_url=[]
    p_name=[]
    ip_url=[]
    run_token=[]
    api1=[]
    p_page = []
    r_name = request.form['runt_drop']
    sql = "SELECT DISTINCT runt FROM input_subcat1 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])
    sql = "SELECT DISTINCT link FROM input_subcat1 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        ip_url.append(t[0])
    sql = "SELECT DISTINCT api FROM input_subcat1 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1.append(t[0])
    sql = "SELECT DISTINCT runt FROM scrap_subcat1 WHERE run_name LIKE '%"+r_name+"%'"
    adr = (r_name,)
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    for t in range(0,len(api1)):
        params = {
                "api_key": api1[t]
                }
        r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[t], params=params)
        y=json.loads(r.text)
        if y['status'] == 'complete':
            continue
        else:
            return " PLEASE WAIT.... Data Not Ready "
    if len(store_var) == 0:
        for z in range(0,len(api1)):
            params = {"api_key": api1[z],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[z]+'/data', params=params)
            d = json.loads(r.text)
            if 'url' in d:
                for t0 in range(0,len(d['url'])):
                    if 'url' in d['url'][t0]:
                        for t1 in range (0,len(d['url'][t0]['url'])):
                            if 'url' in d['url'][t0]['url'][t1]:
                                for t2 in range (0,len(d['url'][t0]['url'][t1]['url'])):
                                    if 'url' in d['url'][t0]['url'][t1]['url'][t2]:
                                        return "Contact Tech Team"
                                    if 'sub_cat1' in d['url'][t0]['url'][t1]['url'][t2]:
                                        for s3 in range (0,len(d['url'][t0]['url'][t1]['url'][t2]['sub_cat1'])):
                                            p_name.append(d['url'][t0]['url'][t1]['url']['sub_cat1'][s3]['name'])
                                            p_url.append(d['url'][t0]['url'][t1]['url']['sub_cat1'][s3]['url'])
                                            p_page.append(d['url'][t0]['url'][t1]['url']['sub_cat1'][s3]['page'])
                            if 'sub_cat1' in d['url'][t0]['url'][t1]:
                                for s2 in range (0,len(d['url'][t0]['url'][t1]['sub_cat1'])):
                                    p_name.append(d['url'][t0]['url'][t1]['sub_cat1'][s2]['name'])
                                    p_url.append(d['url'][t0]['url'][t1]['sub_cat1'][s2]['url'])
                                    p_page.append(d['url'][t0]['url'][t1]['sub_cat1'][s2]['page'])
                    if 'sub_cat1' in d['url'][t0]:
                        for s1 in range (0,len(d['url'][t0]['sub_cat1'])):
                            p_name.append(d['url'][t0]['sub_cat1'][s1]['name'])
                            p_url.append(d['url'][t0]['sub_cat1'][s1]['url'])
                            p_page.append(d['url'][t0]['sub_cat1'][s1]['page'])
            if 'sub_cat1' in d:
                for s0 in range(0,len(d['sub_cat1'])):
                    p_name.append(d['sub_cat1'][s0]['name'])
                    p_url.append(d['sub_cat1'][s0]['url'])
                    p_page.append(d['sub_cat1'][s0]['page'])
        p_len=len(p_name)        
        for i in range(0,p_len):
            v.append((p_name[i],p_url[i],p_page[i],r_name))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_Name', 'Product_URL','Product_Page'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[2]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        sql = "INSERT INTO scrap_subcat1 (product_name,product_url,product_page,run_name) VALUES (%s, %s, %s, %s)"
        mycursor.executemany(sql, v)
        mydb.commit()
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
    else:
        sql = "SELECT product_name, product_url, product_page FROM scrap_subcat1 WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_Name', 'Product_URL','Product_Page','Input_Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[2]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@app.route('/get_data_bs', methods=['POST'])
def get_data_bs():
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_bestseller (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(500), url VARCHAR(800), review VARCHAR(150), review_count VARCHAR(50),price VARCHAR(20),prime VARCHAR(5), ip_link VARCHAR(255), run_name VARCHAR(25))")
    v=[]
    ip_link=[]
    ip_url=[]
    run_token=[]
    api1=[]
    name=[]
    url=[]
    review=[]
    Review_count=[]
    Price=[]
    Prime=[]
    r_name = request.form['runt_drop']
    sql = "SELECT DISTINCT runt FROM input_bestseller WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])
    sql = "SELECT DISTINCT link FROM input_bestseller WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        ip_url.append(t[0])
    sql = "SELECT DISTINCT api FROM input_bestseller WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1.append(t[0])
    sql = "SELECT DISTINCT run_name FROM scrap_bestseller WHERE run_name LIKE '%"+r_name+"%'"
    adr = (r_name,)
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    for t in range(0,len(api1)):
        params = {
                "api_key": api1[t]
                }
        r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[t], params=params)
        y=json.loads(r.text)
        if y['status'] == 'complete':
            continue
        else:
            return " PLEASE WAIT.... Data Not Ready "
    if len(store_var) == 0:
        for z in range(0,len(api1)):
            params = {"api_key": api1[z],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[z]+'/data', params=params)
            y = json.loads(r.text)
            for i in range(0,len(y['list1'])):
                for ii in range(0,len(y['list1'][i])):
                    for iii in range(0,len(y['list1'][i]['Title'])):
                        if 'name' in y['list1'][i]['Title'][iii]:
                            name.append(y['list1'][i]['Title'][iii]['name'])
                        else:
                            name.append("")
                        if 'url' in y['list1'][i]['Title'][iii]:
                            url.append(y['list1'][i]['Title'][iii]['url'])
                        else:
                            url.append("")
                        if 'review' in y['list1'][i]['Title'][iii]:
                            review.append(y['list1'][i]['Title'][iii]['review'])
                        else:
                            review.append("")
                        if 'Review_count' in y['list1'][i]['Title'][iii]:
                            Review_count.append(y['list1'][i]['Title'][iii]['Review_count'])
                        else:
                            Review_count.append("")
                        if 'Price' in y['list1'][i]['Title'][iii]:
                            Price.append(y['list1'][i]['Title'][iii]['Price'])
                        else:
                            Price.append("")
                        if 'Prime' in y['list1'][i]['Title'][iii]:
                            Prime.append("Yes")
                        else:
                            Prime.append("No")
                        ip_link.append(ip_url[i])
        p_len=len(name)
        for i in range(0,p_len):
            v.append((name[i],url[i],review[i],Review_count[i],Price[i],Prime[i],ip_link[i],r_name))
            sql = "INSERT INTO scrap_bestseller (name,url,review,review_count,price,prime,ip_link,run_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor.execute(sql, v[i])
            mydb.commit()
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_Name', 'Product_URL','Product_review','Review_count','Price','Prime','Input_Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[2],
                    item[3],
                    item[4],
                    item[5],
                    item[6]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
    else:
        sql = "SELECT name, url, review, review_count, price, prime, ip_link FROM scrap_bestseller WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2],t[3],t[4],t[5],t[6]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Product_Name', 'Product_URL','Product_review','Review_count','Price','Prime','Input_Link'))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],
                    item[1],
                    item[2],
                    item[3],
                    item[4],
                    item[5],
                    item[6]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@app.route('/get_data_pp', methods=['POST'])
def get_data_pp():
    title =""
    brand =""
    brand_url =""
    pn_mrp =""
    pn_price =""
    pn_you_save =""
    ss_ASIN =""
    ss_bsr =""
    ss_dfa =""
    seller_name =""
    seller_rating =""
    seller_review =""
    description =""
    p_rating =""
    p_rev_count =""
    img_count =""
    img_url =""
    a_prime =""
    a_choise =""
    offers =""
    offers_from =""
    catagory =""
    ip_url=[]
    run_token=[]
    api1=[]
    v=[]
    run_token_sql=[]
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_data_p3 (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(800),brand VARCHAR(800),brand_url VARCHAR(800),pn_mrp VARCHAR(50),pn_price VARCHAR(50),pn_you_save VARCHAR(75),ss_ASIN VARCHAR(50),ss_bsr VARCHAR(50),ss_dfa VARCHAR(50),seller_name VARCHAR(100),seller_rating VARCHAR(75),seller_review VARCHAR(75),description VARCHAR(900),p_rating VARCHAR(250),p_rev_count VARCHAR(100),1_star VARCHAR(10),2_star VARCHAR(10),3_star VARCHAR(10),4_star VARCHAR(10),5_star VARCHAR(10),img_count VARCHAR(25),img_url VARCHAR(800),a_prime VARCHAR(10),a_choise VARCHAR(50),offers VARCHAR(50),offers_from VARCHAR(200),catagory VARCHAR(500),key_pt VARCHAR(900),ip_url VARCHAR(800),run_token  VARCHAR(100),run_name VARCHAR(50))")
    r_name = request.form['runt_drop']
    sql = "SELECT DISTINCT runt FROM input_data_p3 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])
    sql = "SELECT DISTINCT link FROM input_data_p3 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        ip_url.append(t[0])
    sql = "SELECT DISTINCT api FROM input_data_p3 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    api = mycursor.fetchall()
    for t in api:
        api1.append(t[0])
    sql = "SELECT DISTINCT run_name FROM scrap_data_p3 WHERE run_name LIKE '%"+r_name+"%'"
    mycursor.execute(sql)
    store_var = mycursor.fetchall()
    for t in range(0,len(api1)):
        params = {
                "api_key": api1[t]
                }
        r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[t], params=params)
        y=json.loads(r.text)
        if y['status'] == 'complete':
            continue
        else:
            return " PLEASE WAIT.... Data Not Ready "
    if len(store_var) == 0:
        for ap in range(0,len(api1)):
            params = {"api_key": api1[ap],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+run_token[ap]+'/data', params=params)
            y = json.loads(r.text)
            for i in range(0,len(y['list1'])):
                if 'bulk' in y['list1'][i]:
                    d=y['list1'][i]['bulk'].split('\n')
                    for n in range(0,len(d)):
                        if 'Best Sellerin' in d[n]:
                            n1=(d[n].split('Best Sellerin'))
                            ss_bsr = n1[0]
                            catagory = n1[1]
                            break
                        else:
                            ss_bsr = ""
                            catagory = ""
                    for n in range(0,len(d)):
                        if 'Sold by' in d[n]:
                            z=d[n].split('(')
                            z=z[0].split('Sold by ')
                            seller_name=z[1]
                            '''
                            if 'out of 5 ' in d[n]:
                                z=d[n].split('(')
                                z=z[1].split('|')
                                #print("S_Rating :",z[0])
                                seller_rating=z[0]
                                z=z[1].split(')')
                                z=z[0].split()
                                #print("S_Total Rating",z[0])
                                seller_review=z[0]
                            elif 'out of 5 stars' in d[n]:
                                #z=d[n].split('(')
                                #z=z[len(z)-1].split("| ")
                                #print("S_Rating :",z[0])
                                #seller_rating=z[0]
                                #z=z[1].split()
                                #print("S_Total Rating",z[0])
                                #seller_review=z[0]
                                continue
                            '''
                            break
                        else:
                            seller_name=""
                            seller_rating=""
                            seller_review=""
                    for n in range(0,len(d)):
                        if 'Fulfilled by Amazon' in d[n]:
                            a_prime="Yes"
                            break
                        else:
                            a_prime="No"
                    for n in range(0,len(d)):
                        if 'Choice' in d[n]:
                            z=d[n+1].split('"')
                            a_choise=z[1]
                            break
                        else:
                            a_choise=""
                    for n in range(0,len(d)):
                        if 'offers' in d[n]:
                            t=d[n].split()
                            offers=t[0]
                            break
                        else:
                            offers=""
                    for n in range(0,len(d)):
                        if 'offers' in d[n]:
                            t=d[n].split()
                            offers_from=t[len(t)-1]
                            break
                        else:
                            offers_from=""
                    for n in range(0,len(d)):
                        if 'out of' in d[n]:
                            if 'Sold by' in d[n]:
                                break
                            else:
                                p_rating=d[n]
                            break
                        else:
                            p_rating=""
                    for n in range(0,len(d)):
                        if 'customer reviews' in d[n]:
                            z=d[n].split()
                            p_rev_count=z[0]
                            break
                        else:
                            p_rev_count=""
                    for n in range(0,len(d)):
                        if 'offer' in d[n] or 'offers' in d[n]:
                            ky=""
                            for t in range(n+1,len(d)):
                                ky=ky+d[t]
                            key=ky
                            break
                        else:
                            key=""
                if 'title' in y["list1"][i]:
                    title=y["list1"][i]["title"]
                else:
                    title=""
                if 'brand' in y["list1"][i]:
                    brand=y["list1"][i]['brand']
                else:
                    brand=""
                if 'brand_url' in y["list1"][i]:
                    brand_url= y["list1"][i]['brand_url']
                else:
                    brand_url=""
                if "price_note" in (y["list1"][i]):
                    pn_mrp=""
                    pn_price=""
                    pn_you_save=""
                    for np in range (0,len(y["list1"][i]["price_note"])):
                        if "M.R.P" in (y["list1"][i]["price_note"][np]["name"]):
                            pn_mrp = str(y["list1"][i]["price_note"][np]["price"]) 
                        if "You Save" in (y["list1"][i]["price_note"][np]["name"]):
                            pn_you_save=str(y["list1"][i]["price_note"][np]["price"])
                        if "Price" in (y["list1"][i]["price_note"][np]["name"]):
                            pn_price=str(y["list1"][i]["price_note"][np]["price"])
                if "selection2" in (y["list1"][i]):
                    for ns in range(0,len(y["list1"][i]["selection2"])):
                        if "ASIN" ==(y["list1"][i]["selection2"][ns]["name"]):
                            ss_ASIN = y["list1"][i]["selection2"][ns]["product_information"]
                        elif "Amazon Bestsellers Rank" ==(y["list1"][i]["selection2"][ns]["name"]):
                            ss_bsr = y["list1"][i]["selection2"][ns]["product_information"]
                            catagory=ss_bsr=ss_bsr.split("in ")
                            ss_bsr=ss_bsr[0]
                            catagory=catagory[1]
                            catagory=catagory.split('(')
                            catagory=catagory[0]
                        elif "Date First Available" ==(y["list1"][i]["selection2"][ns]["name"]):
                            ss_dfa = y["list1"][i]["selection2"][ns]["product_information"]
                else:
                    ss_ASIN = ""
                    ss_bsr = ""
                    ss_dfa = ""
                if 'rating_stars' in y["list1"][i]:
                    rating_len=len(y["list1"][i]["rating_stars"])
                    rating = ["","","","",""]
                    for nn in range (0,rating_len):
                        if '5 star' in y["list1"][i]["rating_stars"][nn]["name"]:
                            rating[4] = (y["list1"][i]["rating_stars"][nn]["Rating_ration"])
                        elif '4 star' in y["list1"][i]["rating_stars"][nn]["name"]:
                            rating[3] = (y["list1"][i]["rating_stars"][nn]["Rating_ration"])
                        elif '3 star' in y["list1"][i]["rating_stars"][nn]["name"]:
                            rating[2] = (y["list1"][i]["rating_stars"][nn]["Rating_ration"])
                        elif '2 star' in y["list1"][i]["rating_stars"][nn]["name"]:
                            rating[1] = (y["list1"][i]["rating_stars"][nn]["Rating_ration"])
                        elif '1 star' in y["list1"][i]["rating_stars"][nn]["name"]:
                            rating[0] = (y["list1"][i]["rating_stars"][nn]["Rating_ration"])
                        else:
                            continue
                else:
                    rating = ["","","","",""]
                if "description" in (y["list1"][i]):
                    description = (y["list1"][i]["description"])
                else:
                    description = ""
                if "imagecount" in (y["list1"][i]):
                    img_count = str(len(y["list1"][i]["imagecount"]))
                else:
                    img_count = ""
                if 'image' in y["list1"][i]:
                    img_url = y["list1"][i]["image"]
                else:
                    img_url=""
                for nn in range (0,len(y['list1'])):
                        run_token_sql.append(run_token[ap])
                v.append((title ,brand ,brand_url ,pn_mrp ,pn_price ,pn_you_save ,ss_ASIN ,ss_bsr ,ss_dfa ,seller_name ,seller_rating ,seller_review ,description ,p_rating ,p_rev_count ,rating[0],rating[1],rating[2],rating[3],rating[4],img_count,img_url ,a_prime ,a_choise ,offers ,offers_from ,catagory ,key , ip_url[i] , run_token_sql[i], r_name))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(("Title","Brand","Brand_URL","MRP","Price","you_save","ASIN","bsr","dfa","seller_name","seller_rating",
                        "seller_review","description","product_rating","product_rev_count","1_star","2_star","3_star","4_star","5_star",
                        "img_count","img_url","a_prime","a_choise","offers","offers_from","catagory","key"," ip_url"))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],
                    item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],
                    item[17],item[18],item[19],item[20],item[21],item[22],item[23],item[24],
                    item[25],item[26],item[27],item[28],item[29]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        sql = "INSERT INTO scrap_data_p3 (title ,brand ,brand_url ,pn_mrp ,pn_price ,pn_you_save ,ss_ASIN ,ss_bsr ,ss_dfa ,seller_name ,seller_rating ,seller_review ,description ,p_rating ,p_rev_count ,1_star,2_star,3_star,4_star,5_star ,img_count ,img_url ,a_prime ,a_choise ,offers ,offers_from ,catagory ,key_pt , ip_url , run_token, run_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.executemany(sql, v)
        mydb.commit()
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
    else:
        sql = "SELECT title ,brand ,brand_url ,pn_mrp ,pn_price ,pn_you_save ,ss_ASIN ,ss_bsr ,ss_dfa ,seller_name ,seller_rating ,seller_review ,description ,p_rating ,p_rev_count ,1_star,2_star,3_star,4_star,5_star ,img_count ,img_url ,a_prime ,a_choise ,offers ,offers_from ,catagory ,key_pt , ip_url FROM scrap_data_p3 WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],t[14],t[15],t[16],t[17],t[18],t[19],t[20],t[21],t[22],t[23],t[24],t[25],t[26],t[27],t[28]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(("Title","Brand","Brand_URL","MRP","Price","you_save","ASIN","bsr","dfa","seller_name","seller_rating",
                        "seller_review","description","product_rating","product_rev_count","1_star","2_star","3_star","4_star","5_star",
                        "img_count","img_url","a_prime","a_choise","offers","offers_from","catagory","key_pt"," ip_url"))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],
                    item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],
                    item[17],item[18],item[19],item[20],item[21],item[22],item[23],item[24],
                    item[25],item[26],item[27],item[28]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
        
if __name__ == '__main__':
    app.run()