# %%
import os, pandas as pd, xml.etree.ElementTree as ET, re, datetime 
from tkinter import Tk
from tkinter.filedialog import askdirectory, askopenfilename

# %%
def GetChangeset_Errors():
    try:
        file_root = askopenfilename()
        df = pd.read_csv(file_root,sep=';',error_bad_lines= False,skiprows=1)
        source = 'ChangeSet'
        return df,source
    except ValueError:
        print("Error", ValueError)

def GetSummaryReport():
    try:
        root = askdirectory()
        df = pd.DataFrame(columns=['Feeder','Error'])
        
        for i in os.listdir(root):
            with open(root+'/'+i+'/SummaryReport.txt','r') as F:
                Lines = F.readlines()
            for Line in Lines:
                if 'ERROR' in Line:
                    #print('Iteracion ',str(n),i.split('_')[3],Line)
                    
                    df = df.append({'Feeder': i.split('_')[3], 'Error': Line.strip()},ignore_index= True)
        df.drop_duplicates().reset_index().drop(columns='index')
        source = 'Extract'
        return df,source
    except ValueError:
        print("ERROR",ValueError)

def GetSourceFile(FeederCID):
    #for FeederCID in FeederList:
    FileChoosen = ['',0]
    for root,dir,files in os.walk('//10.241.115.13/Extract'):
        for file in files:
            if FeederCID in file:
                FileChoosen[1] = max(FileChoosen[1], os.path.getctime(root+'/'+file))
                FileChoosen[0] = root+'/'+file
                #print(root,file, os.path.getmtime(root + '/'+file))

    return(FileChoosen[0])
    
def GetElementID(path = '',Error_Mess = '',source = ''):
    try:
        root = ET.parse(path)
        for connection in root.findall("{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal"):
            if source == 'ChangeSet':
                if Error_Mess[Error_Mess.find('CustomId =')+3:].split('"')[1].strip() in str(connection.find('{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal.ConnectivityNode').attrib):
                    return 'Revisar elemento:\t' + re.search('\d{15}',str(connection.attrib)).group() + '\tpara solucionar Error:\t' + Error_Mess
            elif source == 'Extract':
                if Error_Mess[Error_Mess.find('customId:')+9:].strip() in str(connection.find('{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal.ConnectivityNode').attrib):
                    return 'Revisar elemento:\t' + re.search('\d{15}',str(connection.attrib)).group() +'\tpara solucionar Error:\t' + Error_Mess
            else: 'No se pudo procesar mensaje'
    except ValueError:
        print(ValueError)

# %%
def main():
    message = """"Procesar:
        1. Changesets rechazados
        2. Extractos Invalidos\n"""
    
    option = input(message)

    if option == '1':
        df,source = GetChangeset_Errors()
        with open('ProcessedErrors_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M')),'w+') as file:
            for i,x in df.iterrows():
                y = GetElementID(GetSourceFile(str(x.Circuit)),x.FileContent,source)       
                if y != None:
                    file.write('Feeder:\t'+str(x.Circuit)+'\t'+str(y)+'\n')       
        file.close()
        print('Se ha finalizado la ejecución, el archivo se encuentra en {}'.format(os.getcwd()))

    elif option == '2':
        df,source = GetSummaryReport()
        df.to_csv('.\SummaryProcessed.csv',index=False)

if __name__ == '__main__':
    main()

# %%



