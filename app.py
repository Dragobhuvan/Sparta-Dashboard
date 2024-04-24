import datetime
from flask import Flask, render_template, request
#CDi imports
import pandas as pd
import query
import os
from pendulum import today
from dateutil.parser import parse
#Accrual imports 
import pyodbc
from datetime import datetime
from multiprocessing import connection
from sqlite3 import Cursor
from tkinter.tix import COLUMN
from unicodedata import decimal
from decimal import Decimal



app = Flask(__name__)

@app.route('/')
def dashboard():
    return render_template('index.html')


#----------------CDI section-----------------#
#Route for rendering the splitter index page
@app.route('/splitter')
def cdi_auto():
    return render_template('CDI_index.html')


#This CDi splitter logic
@app.route('/cdi_split', methods=['GET','POST'])
def cdi_split():
    if request.method == 'POST':
        # Get the uploaded file
        uploaded_file = request.files['file']

        # Get the output folder path
        output_folder = request.form['output_folder']
 
        # Save the uploaded file to a temporary location
        file_path = os.path.join('uploads', uploaded_file.filename)
        uploaded_file.save(file_path)

        # Call your function to process the CDI errors
        process_cdi_errors_today(file_path, output_folder)
 
        return "CDI errors processed and saved successfully!"

# this method is the logic for splitting the CDI files
date = today().strftime("%Y%m%d") 
def process_cdi_errors_today(file_path, output_path, hrs_file_name=("HRS_Backlogs_" + date + ".csv"), lab_file_name=("Labour_BAU_Backlogs_" + date + ".csv")) :
    df = pd.read_csv(file_path, encoding="cp1252")

    rows = df.shape[0]  
 
    df["COLLEAGUE ID"].fillna("0", inplace=True)
    try : df['COLLEAGUE ID'] = df['COLLEAGUE ID'].astype("int")
    except ValueError : pass
    df['COLLEAGUE ID'] = df['COLLEAGUE ID'].astype("string")
 
    df['COLLEAGUE ID'] = df['COLLEAGUE ID'].replace("0", "", limit=1)
 
    df['DESCRIPTION'] = df['DESCRIPTION'].astype("string")
    df["DESCRIPTION"].fillna("", inplace=True)
 
    df['ERROR DATE & TIME'] = pd.to_datetime(df['ERROR DATE & TIME']).dt.date
 
    df['FIELD NAME ERROR'] = df['FIELD NAME ERROR'].astype("string")
    df["FIELD NAME ERROR"].fillna("", inplace=True)
 
    df['ID'] = df['ID'].astype("string")
    df["ID"].fillna("", inplace=True)
 
    df['TRANSACTION TYPE'] = df['TRANSACTION TYPE'].astype("string")
    df["TRANSACTION TYPE"].fillna("", inplace=True)
 
    # Reordering the table columns
    df = df[["COLLEAGUE ID", "ERROR DATE & TIME", "DESCRIPTION", "TRANSACTION TYPE", "ERROR NUMBER", "FIELD NAME ERROR", "ID" ]]
 
    # Adding Summary and Teams & Actions columns; Data filled through function
    df["Summary"]        = [query.get_summary(row["DESCRIPTION"], row["TRANSACTION TYPE"]) for index, row in df.iterrows()]
    df["Team & Actions"] = [query.get_team_and_action(row["Summary"]) for index2, row in df.iterrows()]
    hrs = df[df["Team & Actions"].str.contains('HRS')]
    lab_bau = df[df["Team & Actions"].str.contains('Labour')]
    
    if len(output_path) > 0 :
        hrs.to_csv(output_path + "/" + hrs_file_name, index=False)
        lab_bau.to_csv(output_path + "/" + lab_file_name, index=False)
    else :
        hrs.to_csv(hrs_file_name, index=False)
        lab_bau.to_csv(lab_file_name, index=False)


#---------------------ACCRUAL section---------------#

#to render the index page of accrual app
@app.route('/accrual_app')
def acc_app():
    return render_template('ACC_new_index.html')

#----To render features of accrual app-----
@app.route('/taken')
def taken():
    cidInvalid = False
    return render_template('new_taken.html', cidInvalid = cidInvalid)

@app.route('/earned')
def earned():
    return render_template('new_earned.html')

@app.route('/reset')
def reset():
    return render_template('new_reset.html')

@app.route('/allthree')
def allthree():
    return render_template('new_Mreset.html')

@app.route('/storereset')
def storeres():
    return render_template('new_storeres.html')

#------functionalties of accrual features----#

#Taken
@app.route('/tkn', methods=['GET','POST'])
def Taken_count():
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    #To transform the date time local to yyyy-mm-dd hh:mm:ss format
    fromdate =  request.form.get('fromdate')
    fromdate_obj = datetime.fromisoformat(fromdate.replace('T', ' '))
    fromdate_modified = fromdate_obj.strftime('%Y-%m-%d %H:%M:%S')

    tilldate =  request.form.get('todate')
    tilldate_obj = datetime.fromisoformat(tilldate.replace('T', ' '))
    tilldate_modified = tilldate_obj.strftime('%Y-%m-%d %H:%M:%S')
    
    colleague_id = request.form.get('CID')
    
    cidInValid = False

    if(isValid(colleague_id) == False):
        cidInValid = True
        return render_template('new_taken.html', cidInValid = cidInValid )
    
   
    # a=cursor.execute("select count(*) from WDMDEVICE where ENABLEDSW ='1'")
    cursor.execute("select SUM (amount/3600), EFFECTIVEDATE from ACCRUALTRAN A join PERSON P on P.PERSONID = A.PERSONID where ACCRUALCODEID = 2 and EFFECTIVEDATE BETWEEN ? AND ? and TYPE = 1 and P.PERSONNUM IN (?) GROUP BY TYPE, EFFECTIVEDATE", fromdate_modified,tilldate_modified,colleague_id)
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
    lst = []
    for i in results:
        for j in columns:
            lst.append(i[j])

    
    decimals = [item for item in lst if isinstance(item, Decimal)]
    total_taking = sum(decimals)
    total_taking ='{:.2f}'.format(total_taking)

    for result in results:
        for key, value in result.items():
            if isinstance(value, Decimal):
                result[key] = '{:.2f}'.format(value)
                
    print("this is the result of fetchall() zip", results)
    
    return render_template('TKN_result.html',  columns=columns, results=results, total_taking = total_taking, colleague_id = colleague_id)
    # return render_template('TKN_result.html')

#Earned

@app.route('/adj', methods=['GET','POST'])
def Earned_count():
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    
    fromdate =  request.form.get('fromdate')
    fromdate_obj = datetime.fromisoformat(fromdate.replace('T', ' '))
    fromdate_modified = fromdate_obj.strftime('%Y-%m-%d %H:%M:%S')

    tilldate =  request.form.get('todate')
    tilldate_obj = datetime.fromisoformat(tilldate.replace('T', ' '))
    tilldate_modified = tilldate_obj.strftime('%Y-%m-%d %H:%M:%S')
    
    colleague_id = request.form.get('CID')
    
    cidInValid = False

    if(isValid(colleague_id) == False):
        cidInValid = True
        return render_template('new_earned.html', cidInValid = cidInValid )
        
    cursor.execute("select SUM (amount/3600), EFFECTIVEDATE from ACCRUALTRAN A join PERSON P on P.PERSONID = A.PERSONID where ACCRUALCODEID = 2 and EFFECTIVEDATE BETWEEN ? AND ? and TYPE = 2 and P.PERSONNUM IN (?) GROUP BY TYPE, EFFECTIVEDATE", fromdate_modified,tilldate_modified,colleague_id)
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
    
    lst = []
    for i in results:
        for j in columns:
            lst.append(i[j])
     

    decimals = [item for item in lst if isinstance(item, Decimal)]
    total_earned = sum(decimals)
    total_earned ='{:.2f}'.format(total_earned)
    
    for result in results:
        for key, value in result.items():
            if isinstance(value, Decimal):
                result[key] = '{:.2f}'.format(value)
    
    return render_template('ADJ_result.html',  columns=columns, results=results, total_earned = total_earned,colleague_id=colleague_id )

#Reset
@app.route('/rst', methods=['GET','POST'])
def Reset_count():
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    fromdate =  request.form.get('fromdate')
    fromdate_obj = datetime.fromisoformat(fromdate.replace('T', ' '))
    fromdate_modified = fromdate_obj.strftime('%Y-%m-%d %H:%M:%S')

    tilldate =  request.form.get('todate')
    tilldate_obj = datetime.fromisoformat(tilldate.replace('T', ' '))
    tilldate_modified = tilldate_obj.strftime('%Y-%m-%d %H:%M:%S')
    
    colleague_id = request.form.get('CID')
    
    cidInValid = False

    if(isValid(colleague_id) == False):
        cidInValid = True
        return render_template('new_taken.html', cidInValid = cidInValid )
   
    cursor.execute("select SUM (amount/3600), EFFECTIVEDATE from ACCRUALTRAN A join PERSON P on P.PERSONID = A.PERSONID where ACCRUALCODEID = 2 and EFFECTIVEDATE BETWEEN ? AND ? and TYPE = 3 and P.PERSONNUM IN (?) GROUP BY TYPE, EFFECTIVEDATE", fromdate_modified,tilldate_modified,colleague_id)
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
    lst = []
    for i in results:
        for j in columns:
            lst.append(i[j])
     

    decimals = [item for item in lst if isinstance(item, Decimal)]
    total_reset = sum(decimals)
    total_reset ='{:.2f}'.format(total_reset)
    
    for result in results:
        for key, value in result.items():
            if isinstance(value, Decimal):
                result[key] = '{:.2f}'.format(value)
    

    return render_template('RST_result.html',  columns=columns, results=results, total_reset = total_reset, colleague_id = colleague_id)


#Multiple resets
@app.route('/mrst', methods=['GET','POST'])
def M_rst_count():
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    fromdate =  request.form.get('fromdate')
    fromdate_obj = datetime.fromisoformat(fromdate.replace('T', ' '))
    fromdate_modified = fromdate_obj.strftime('%Y-%m-%d %H:%M:%S')

    tilldate =  request.form.get('todate')
    tilldate_obj = datetime.fromisoformat(tilldate.replace('T', ' '))
    tilldate_modified = tilldate_obj.strftime('%Y-%m-%d %H:%M:%S')
   
    cursor.execute("select P.PERSONNUM,SUM (amount/3600),EFFECTIVEDATE from ACCRUALTRAN A join PERSON P on P.PERSONID = A.PERSONID where ACCRUALCODEID = 2 and EFFECTIVEDATE BETWEEN ? AND ? and TYPE = 3 GROUP BY PERSONNUM,EFFECTIVEDATE having count(type) >=1 ORDER BY PERSONNUM,EFFECTIVEDATE", fromdate_modified,tilldate_modified)
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
    lst = []
    for i in results:
        for j in columns:
            lst.append(i[j])
     

    decimals = [item for item in lst if isinstance(item, Decimal)]
    
    for result in results:
        for key, value in result.items():
            if isinstance(value, Decimal):
                result[key] = '{:.2f}'.format(value)
    

    return render_template('MRST_result.html',  columns=columns, results=results, fromdate=fromdate, tilldate=tilldate)

#Store resets
@app.route('/storeres', methods=['GET','POST'])
def storeres_counts():
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    
    store_id1 = request.form.get('SID')
    
    store_id = "A" + store_id1  
    
    cursor.execute("select PERSONNUM from VP_ALLPERSONV42 where HOMELABORLEVELNM5 = ? and EMPLOYMENTSTATUS = 'Active'", store_id )
    
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
    lst = []
    for i in results:
        for j in columns:
            lst.append(i[j])
                
    query = "select P.PERSONNUM,SUM (amount/3600), EFFECTIVEDATE from ACCRUALTRAN A join PERSON P on P.PERSONID = A.PERSONID where ACCRUALCODEID = 2 and EFFECTIVEDATE BETWEEN '2024-03-03 00:00:00.000' AND '2025-03-01 00:00:00.000' and TYPE = 3 and P.PERSONNUM IN ({}) GROUP BY TYPE,PERSONNUM, EFFECTIVEDATE ORDER BY PERSONNUM".format(','.join('?' for _ in lst))

    cursor1.execute(query, lst)

    columns1 = [column[0] for column in cursor1.description]
    results1 = [dict(zip(columns1,row)) for row in cursor1.fetchall()]
    

    return render_template('new_Storeresult.html',  columns=columns1, results=results1, store_id=store_id)

def isValid(cid):
    connection_string = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=10.124.128.108;"
                "Database=tkcsdb;"
                "UID=ODBC_jsinfo;"
                "PWD=%o0VO3ixiLZLB36ZkncZ;")
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    cursor.execute("select * from VP_ALLPERSONV42 where PERSONNUM = ?", cid)
    
    results = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    if not results:
        return False
    else:
        return True
    
    

#-------------Payroll section---------------#
@app.route('/payroll_app')
def pyrl_app():
    return render_template('PYRL_index.html')




@app.route('/newacc')
def new_acc_app():
    return render_template('Acc_new_index.html')

if __name__ == '__main__':
    app.run(debug=True, port = 5002)
