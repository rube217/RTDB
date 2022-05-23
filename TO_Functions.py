# %%
from importlib.resources import path
import os, pyodbc,pandas as pd, xml.etree.ElementTree as ET, re, datetime, xmltodict
from tkinter import Tk
from tkinter.filedialog import askdirectory, askopenfilename

# %%
def GetDicts(df=pd.DataFrame()):
    try:
        if df.empty:
            query_file = "./Config/Queries.xml"
            with open(query_file,"r") as xmlFile:
                xmldict = xmltodict.parse(xmlFile.read())
            dict_table = {}
            for i in xmldict['tables']['table']:
                dict_table.update({i['id']:[i['Query'],i['Name']]})
            return dict_table
            
        else:
            x = list(df.loc[:,df.columns.str.contains('_x')].columns)
            y = list(df.loc[:,df.columns.str.contains('_y')].columns)
            normal = [i[:-2] for i in x]
            dictx = dict(zip(x,normal))
            dicty = dict(zip(y,normal))

            return dictx,dicty
    except ValueError:
        print(ValueError)


def GetDifferencesRTDB(conn_prod, conn_dev, table):
    
    dict_table = GetDicts()

    try:
        table_dev = pd.read_sql_query(dict_table[table][0], conn_dev, coerce_float = False)
        table_prod = pd.read_sql_query(dict_table[table][0], conn_prod, coerce_float = False)

        if (table_dev.columns == 'description').any():
            table_dev['description']=table_dev['description'].str.strip()
        if (table_prod.columns == 'description').any():
            table_prod['description']=table_prod['description'].str.strip()

        table_diff = pd.merge(table_dev,table_prod, on='Name' ,how='outer', indicator='Exist')
        dictx, dicty = GetDicts(table_diff)
        
        table_delete = table_diff.loc[table_diff['Exist']=='left_only',(table_diff.columns == 'Name') | (table_diff.columns.str.contains('_x'))].rename(columns = dictx)
        table_create = table_diff.loc[table_diff['Exist']=='right_only',(table_diff.columns == 'Name') | (table_diff.columns.str.contains('_y'))].rename(columns = dicty)
        
        table_update = table_diff.loc[table_diff['Exist']=='both',:].reset_index()
        x = table_update.loc[:,(table_update.columns == 'Name')|(table_update.columns.str.contains('_x'))].rename(columns=dictx)
        y = table_update.loc[:,(table_update.columns == 'Name')|(table_update.columns.str.contains('_y'))].rename(columns=dicty)
        table_update = y.loc[~(x==y).all(1)]
               
        if not(table_update.empty & table_create.empty & table_delete.empty):
            with pd.ExcelWriter('{}_{}.xls'.format(dict_table[table][1],datetime.datetime.now().strftime('%Y%m%d_%H%M'))) as writer:
                if not(table_update.empty & table_create.empty):
                    pd.concat([table_update,table_create]).to_excel(writer,sheet_name=dict_table[table][1], index=False)
                if not(table_delete.empty):
                    table_delete.to_excel(writer,sheet_name='{}_delete'.format(dict_table[table][1]), index=False)
                writer.save()
            print("Se ha generado con exito el archivo excel:{}/{}_{}.xls, por favor subirlo al ADE en Dev".format(os.path.realpath(__file__),dict_table[table][1],datetime.datetime.now().strftime('%Y%m%d_%H%M')))
        else:
            print("No hay diferencias entre Dev y Prod")
        
    except KeyError:
        print("El valor {} no es permitido, los valores permitidos son: status, analog, rate, multistate, station, remote, connection".format(table))

# %%
def GetSummaryReport():
    try:
        root = askdirectory()
        df = pd.DataFrame(columns=['Feeder','Error'])
        n= 0
        for i in os.listdir(root):
            with open(root+'/'+i+'/SummaryReport.txt','r') as F:
                Lines = F.readlines()
            for Line in Lines:
                if 'ERROR' in Line:
                    #print('Iteracion ',str(n),i.split('_')[3],Line)
                    n +=1
                    df = df.append({'Feeder': i.split('_')[3], 'Error': Line.strip()},ignore_index= True)
        df.drop_duplicates().reset_index().drop(columns='index')
        return df
    except ValueError:
        print("ERROR",ValueError)

def GetSourceFile(FeederList):
    for FeederCID in FeederList:
        FileChoosen = ['',0]
        for root,dir,files in os.walk('//10.241.115.13/Extract'):
            for file in files:
                if FeederCID in file:
                    FileChoosen[1] = max(FileChoosen[1], os.path.getctime(root+'/'+file))
                    FileChoosen[0] = root+'/'+file

    return(FileChoosen[0])
    
def GetElementID(path = '',Error_Mess = ''):
    try:
        root = ET.parse(path)
        for connection in root.findall("{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal"):
            if Error_Mess[Error_Mess.find('Id:')+3:].strip() in str(connection.find('{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal.ConnectivityNode').attrib):
                print(re.search('\d{15}',str(connection.attrib)).group())
    except ValueError:
        print(ValueError)

# %%
conn_prod = pyodbc.connect('Driver={SQL Server}; Server=10.241.109.41,20010\\OASYSHDB;Database=EPSA_Reporting;UID=epsareportes; PWD=Epsa.2020!;')
conn_dev = pyodbc.connect('Driver={SQL Server}; Server=10.241.114.12,20010\\OASYSHDB;Database=ADMS_QueryEngine;UID=Epsareportes; PWD=cmXoasys2;')

message = """"
Por favor ingresa el numero de la Tabla deseas comparar entre Dev y Production:

1. status
2. analog
3. rate
4. multistate
5. connection
6. remote
7. station
"""
# %%
GetDifferencesRTDB(conn_prod,conn_dev,input(message))