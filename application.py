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
pt_list_pp =['tVod50sgHR1G','tEOkkeiohWp7','tHqw2wfTx-04','t8D_pSgpTG4Y','t79kB_a4zd6m','thJTnuPpfZsU','tT3PcfeGdrBn',
             'tPnM39aaC-nz','tTWFbiEo4yw3','t7v7F31s-ACt','t4Y2Nq26F-sE','tk-T22vtVckS','tOeETVP49LT-','tzsPHeUdJXf_',
             'tz_hf2nMxCoB','tczkJPkCmdei','tiGuHPWFuKGK','tLzz2AChGTTC','tAv5UGNEt2G1','tJCpxUfoVTj2','trZ7eifV7dT1',
             'tRPqGaDc_5wt','t2rCm07szXuq','tCrVVZSsDTSN','txq6xcCfpVOW','tXtzGnZs2jBJ','tR1raW8NEohe','txsXiva-b3RS',
             'tbGVyHUTCF-w','tKYPQUXGWS9L']
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
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    api=[]
    pt=[]
    v=[]    
    link= request.form['link_pl']
    link1=link.split()
    link_len=len(link1)
    if link_len != 0:
        if link_len < 199:
            api_sel= 1
            api_r = 0
        elif link_len > 5970:
            api_sel=30
            api_r=0
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
    ft=api_sel+api_r
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
        if len(api) == (ft):
            break
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
    
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
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
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_subcat1 (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")

    api=[]
    pt=[]
    v=[]    
    link= request.form['link_sc']
    link1=link.split()
    link_len=len(link1)
    if link_len != 0:
        if link_len < 199:
            api_sel= 1
            api_r = 0
        elif link_len > 5970:
            api_sel=30
            api_r=0
        else:
            api_sel = int(link_len/199)
            if link_len%199 != 0:
                api_r = 1
            else:
                api_r = 0
    ft=api_sel+api_r
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
        if len(api) == (ft):
            break
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
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
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
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_bestseller (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    api=[]
    pt=[]
    v=[]    
    link= request.form['link_bs']
    link1=link.split()
    link_len=len(link1)
    if link_len != 0:
        if link_len < 99:
            api_sel= 1
            api_r = 0
        elif link_len > 2970:
            api_sel=30
            api_r=0
        else:
            api_sel = int(link_len/99)
            if link_len%99 != 0:
                api_r = 1
            else:
                api_r = 0
    ft=api_sel+api_r
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
        if len(api) == (ft):
            break
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
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
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
    mycursor.execute("CREATE TABLE IF NOT EXISTS input_data_p3 (id INT AUTO_INCREMENT PRIMARY KEY, api VARCHAR(25), pt VARCHAR(25), link VARCHAR(255), runt VARCHAR(25), run_name VARCHAR(25))")
    api=[]
    pt=[]
    v=[]    
    link= request.form['link_pp']
    link1=link.split()
    link_len=len(link1)

    if link_len < 99:#100:
        ft= 1
    elif link_len > 2970: #5970 :
        ft=31
    else:
        apl = int(link_len/99)#199)
        if link_len%99 != 0:#199
            apr = 1
        else:
            apr = 0
        ft=apl+apr
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
        alen=len(api)
        if alen == (ft):
            break
    r_name= request.form['r_name']
    if r_name == "":
        return "Error : Please Enter Valid Run Name"
    max_link = len(api)*99#199
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
        if link_len < 99:#199:
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
            count_max_temp=99#199
        else:
            api_sel = int(link_len/99)#199)
            if link_len%99 != 0:#199
                api_r = 1
            else:
                api_r = 0
            count_start = 0
            count_max=link_len
            count_max_temp=99#199
            count_start_sql = 0
            count_max_sql=99#199
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
                if (link_len-count_start) >= 99:#199:
                    count_max_temp = count_max_temp + 99#199
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
                    if (link_len-count_start_sql) > 99:#199:
                        count_max_sql=count_max_sql+99#199
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
                if (link_len-count_start) > 99:#199:
                    count_max_temp = count_max_temp+99#199
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
                    if (link_len-count_start_sql) > 99:#199:
                        count_max_sql=count_max_sql+99#199
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
                            p_name=(y['details'][n]['selection1'][nn]['name'])
                            p_url=(y['details'][n]['selection1'][nn]['url'])
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        ip_link=(ip_url[n])
                    for nn in range (0,len(y['details'][n]['selection1'])):
                        run_token_sql=(run_token[z])
                else:
                    continue
            v=[]
            v.append((p_name,p_url,run_token_sql,ip_link,r_name))
            sql = "INSERT INTO scrap_data (product_name,product_url,runt,ip_link,run_name) VALUES (%s, %s, %s, %s, %s)"
            mycursor.executemany(sql, v)
            mydb.commit()
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
            v=[]
            v.append((name[i],url[i],review[i],Review_count[i],Price[i],Prime[i],ip_link[i],r_name))
            sql = "INSERT INTO scrap_bestseller (name,url,review,review_count,price,prime,ip_link,run_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            mycursor.executemany(sql, v)
            mydb.commit()
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
    ip_url=[]
    run_token=[]
    api1=[]
    page_not_found=[]
    i_link=0
    v=[]
    pn_mrp=""
    pn_price=""
    pn_you_save=""

    t_store=[]
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS scrap_data_p3 (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(800),brand VARCHAR(800),p_rating VARCHAR(250),p_rev_count VARCHAR(100),pn_mrp VARCHAR(50), pn_price VARCHAR(50),pn_you_save VARCHAR(75),seller_name VARCHAR(100),img_count VARCHAR(25),keypt_count VARCHAR(25),description VARCHAR(900),ss_ASIN VARCHAR(50),k_word VARCHAR(150),pos VARCHAR(10),ss_bsr VARCHAR(50),ss_bsr_cat VARCHAR(500),ss_dfa VARCHAR(50),a_prime VARCHAR(10),1_star VARCHAR(10),2_star VARCHAR(10),3_star VARCHAR(10),4_star VARCHAR(10),5_star VARCHAR(10),ip_url VARCHAR(800),run_token  VARCHAR(100),run_name VARCHAR(50))")
    r_name = request.form['runt_drop']
    #r_name="test"
    sql = "SELECT DISTINCT runt FROM input_data_p3 WHERE run_name = %s"
    adr = (r_name,)
    mycursor.execute(sql, adr)
    ip_l = mycursor.fetchall()
    for t in ip_l:
        run_token.append(t[0])
    sql = "SELECT link FROM input_data_p3 WHERE run_name = %s"
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
            t_store.append(r.text)
    if len(store_var) == 0:
        for ap in range(0,len(t_store)):                
            y = json.loads(t_store[ap])
            for i in range(0,len(y['list1'])):
                if 'title' in y["list1"][i]:
                    title=y["list1"][i]["title"]
                else:
                    if 'page' in y['list1'][i]:
                        page_not_found.append(y["list1"][i]['page'])
                    title=""
                if 'brand_name' in y["list1"][i]:
                    brand=y["list1"][i]['brand_name']
                else:
                    brand=""
                if 'rating' in y["list1"][i]:
                    p_rating = y["list1"][i]["rating"]
                else:
                    p_rating=""
                if 'rating_count' in y["list1"][i]:
                    rating_count = y["list1"][i]["rating_count"]
                else:
                    rating_count=""
                if "pricetag" in y["list1"][i]:
                    pn_mrp=""
                    pn_price=""
                    pn_you_save=""
                    for np in range (0,len(y["list1"][i]["pricetag"])):
                        if "M.R.P" in (y["list1"][i]["pricetag"][np]["name"]):
                            pn_mrp = str(y["list1"][i]["pricetag"][np]["price"]) 
                        if "You Save" in (y["list1"][i]["pricetag"][np]["name"]):
                            pn_you_save=str(y["list1"][i]["pricetag"][np]["price"])
                        if "Price" in (y["list1"][i]["pricetag"][np]["name"]):
                            pn_price=str(y["list1"][i]["pricetag"][np]["price"])
                if 'buy_box' in y["list1"][i]:
                    buy_box = y["list1"][i]["buy_box"]
                else:
                    buy_box=""
                if "images" in (y["list1"][i]):
                    img_count = str(len(y["list1"][i]["images"]))
                else:
                    img_count = "0"
                if "keypoints" in (y["list1"][i]):
                    keypoints_count = str(len(y["list1"][i]["keypoints"]))
                else:
                    keypoints_count = ""
                if "description" in (y["list1"][i]):
                    description = (y["list1"][i]["description"])
                else:
                    description = ""
                if "prime" in (y["list1"][i]):
                    a_prime = "Yes"
                else:
                    a_prime = "No"
                if "details4" in (y["list1"][i]):
                    ss_ASIN = ""
                    ss_bsr = ""
                    ss_dfa = ""
                    catagory=""
                    for ns in range(0,len(y["list1"][i]["details4"])):
                        if "ASIN" in (y["list1"][i]["details4"][ns]["name"]):
                            ss_ASIN = y["list1"][i]["details4"][ns]["name"]
                            ss_ASIN = ss_ASIN.split('ASIN')
                            if ': ' in ss_ASIN[1]:                
                                ss_ASIN = ss_ASIN[1].split(': ')
                                ss_ASIN = ss_ASIN[1]
                            else:
                                ss_ASIN=""
                        elif "Amazon Bestsellers Rank" in (y["list1"][i]["details4"][ns]["name"]):
                            ss_bsr = y["list1"][i]["details4"][ns]["name"]
                            catagory=ss_bsr=ss_bsr.split(":")
                            ss_bsr=ss_bsr[1]
                            catagory=ss_bsr[1]
                        elif "Date First Available" in (y["list1"][i]["details4"][ns]["name"]):
                            ss_dfa = y["list1"][i]["details4"][ns]["name"]
                            ss_dfa = ss_dfa.split(': ')
                            ss_dfa = ss_dfa[1]
                else:
                    ss_ASIN = ""
                    ss_bsr = ""
                    ss_dfa = ""
                    catagory=""
                if 'stars' in y["list1"][i]:
                    rating_len=len(y["list1"][i]["stars"])
                    rating = ["","","","",""]
                    for nn in range (0,rating_len):
                        if '5 star' in y["list1"][i]["stars"][nn]["name"]:
                            rating[4] = (y["list1"][i]["stars"][nn]["stars_split"])
                        elif '4 star' in y["list1"][i]["stars"][nn]["name"]:
                            rating[3] = (y["list1"][i]["stars"][nn]["stars_split"])
                        elif '3 star' in y["list1"][i]["stars"][nn]["name"]:
                            rating[2] = (y["list1"][i]["stars"][nn]["stars_split"])
                        elif '2 star' in y["list1"][i]["stars"][nn]["name"]:
                            rating[1] = (y["list1"][i]["stars"][nn]["stars_split"])
                        elif '1 star' in y["list1"][i]["stars"][nn]["name"]:
                            rating[0] = (y["list1"][i]["stars"][nn]["stars_split"])
                        else:
                            continue
                else:
                    rating = ["","","","",""]
                    
                pros=ip_url[i_link]
                if "keywords=" in pros:
                    k_word=pros.split('keywords=')
                    k_word=k_word[1]
                    k_word=k_word.split('&')
                    if '+' in k_word[0]:
                        k_word=k_word[0].replace("+", " ")
                if 'ref=sr_1_' in pros:
                    pos=pros.split('ref=sr_1_')
                    pos=pos[1]
                    pos=pos.split('?')
                    pos=pos[0]
                if title !="" :
                    v=[]
                    v.append((title ,brand ,p_rating ,rating_count ,pn_mrp ,pn_price ,pn_you_save ,buy_box ,img_count ,keypoints_count ,description ,ss_ASIN ,k_word ,pos ,ss_bsr ,catagory ,ss_dfa ,a_prime ,rating[0],rating[1],rating[2],rating[3],rating[4], ip_url[i_link] , run_token[ap], r_name))
                    sql = "INSERT INTO scrap_data_p3 (title ,brand ,p_rating ,p_rev_count ,pn_mrp ,pn_price ,pn_you_save ,seller_name ,img_count ,keypt_count ,description ,ss_ASIN , k_word ,pos ,ss_bsr ,ss_bsr_cat ,ss_dfa ,a_prime ,1_star ,2_star ,3_star ,4_star ,5_star ,ip_url ,run_token ,run_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    mycursor.executemany(sql, v)
                    mydb.commit()

                i_link=i_link+1
                
        if len(page_not_found) == 0:
            return "No missing data found All Data Written to DB successfully"
        else:
            y=""
            for i in page_not_found:
                y= y+str(i) +"</br>"
            return y

        '''sql = "SELECT title ,brand ,p_rating ,p_rev_count ,pn_mrp ,pn_price ,pn_you_save ,seller_name ,img_count ,keypt_count ,description ,ss_ASIN ,ss_bsr ,ss_bsr_cat ,ss_dfa ,a_prime ,1_star ,2_star ,3_star ,4_star ,5_star ,ip_url FROM scrap_data_p3 WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],t[14],t[15],t[16],t[17],t[18],t[19],t[20],t[21]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Title','Brand','Rating','Rating_count','MRP','Price','Discount','Buy_box','Image_count','Keypoints_count',
                        'Description','ASIN','Bsr_rank','Bsr_cat','Date_First_Available','Prime','1_star','2_star','3_star','4_star',
                        '5_star',"Ip_url"))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],
                    item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],
                    item[17],item[18],item[19],item[20],item[21]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response'''
    else:
        sql = "SELECT title ,brand ,p_rating ,p_rev_count ,pn_mrp ,pn_price ,pn_you_save ,seller_name ,img_count ,keypt_count ,description ,ss_ASIN ,k_word ,pos ,ss_bsr ,ss_bsr_cat ,ss_dfa ,a_prime ,1_star ,2_star ,3_star ,4_star ,5_star ,ip_url FROM scrap_data_p3 WHERE run_name LIKE '%"+r_name+"%'"
        adr = (r_name,)
        mycursor.execute(sql)
        store_var = mycursor.fetchall()
        for t in store_var:
            v.append((t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],t[11],t[12],t[13],t[14],t[15],t[16],t[17],t[18],t[19],t[20],t[21],t[22],t[23]))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(('Title','Brand','Rating','Rating_count','MRP','Price','Discount','Buy_box','Image_count','Keypoints_count',
                        'Description','ASIN','Keyword','Position','Bsr_rank','Bsr_cat','Date_First_Available','Prime','1_star','2_star','3_star','4_star',
                        '5_star',"Ip_url"))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],
                    item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],
                    item[17],item[18],item[19],item[20],item[21],item[22],item[23]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
        
if __name__ == '__main__':
    app.run()