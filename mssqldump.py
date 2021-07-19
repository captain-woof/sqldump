#!/usr/bin/python3

from argparse import ArgumentParser
import pymssql
from prettytable import PrettyTable

def getConnCursor(database_name,print_banner=False):
    if print_banner:
        print("[>] Connecting to {}:{}".format(host,port))
    conn = pymssql.connect(server=host,port=port,user=username,password=password,database=database_name)
    cursor = conn.cursor(as_dict=True)
    return conn,cursor

def enumDatabases():
    # Connect to server
    printAndWriteToFile("\n[+] Enumerating database names")

    # Enumerate
    cursor.execute("SELECT name FROM master.dbo.sysdatabases")
    printAndWriteToFile("Databases found: ",end="")
    for row in cursor:
        printAndWriteToFile(row['name'],end=", ")
        data_struct[row['name']] = None
    printAndWriteToFile("")

def enumTables():
    printAndWriteToFile("\n[+] Enumerating tables in each database")
    for i,database_name in enumerate(data_struct.keys()):
        try:
            printAndWriteToFile("#{}. {} => ".format(i,database_name),end="")
            # Enumerate
            cursor.execute("SELECT table_name FROM {}.information_schema.tables".format(database_name))
            table_names = {}
            for row in cursor:
                table_names[row['table_name']] = None
            if len(list(table_names.keys())) != 0:
                data_struct[database_name] = table_names
                for table_name in table_names.keys():
                    printAndWriteToFile(table_name,end=", ")
            else:
                data_struct[database_name] = None
                printAndWriteToFile(r"{None found}",end="")
        except:
            printAndWriteToFile(r"{Error}",end="")
        finally:
            printAndWriteToFile("")

def enumColumns():
    printAndWriteToFile("\n[+] Enumerating column names of each table")
    for database_name in data_struct.keys():
        if data_struct[database_name] is not None:
            for table_name in data_struct[database_name].keys():
                try:
                    printAndWriteToFile("# {}.{} => ".format(database_name,table_name),end="")                    
                    cursor.execute("SELECT column_name FROM {}.information_schema.columns WHERE table_name='{}'".format(database_name,table_name))
                    column_names = []
                    for row in cursor:
                        column_name = row['column_name']
                        column_names.append(column_name)
                    if len(column_names) != 0:
                        data_struct[database_name][table_name] = column_names
                        for column_name in column_names:
                            printAndWriteToFile(column_name,end=", ")
                    else:
                        printAndWriteToFile(r"{None found}",end="")
                except:
                    pass
                finally:                    
                    printAndWriteToFile("")

def dumpData():
    printAndWriteToFile("\n[+] Dumping all accessible data")
    for database_name in data_struct.keys():        
        if data_struct[database_name] is not None:
            connection,cur = getConnCursor(database_name)
            for table_name in data_struct[database_name].keys():
                print("\n{}.{}:".format(database_name,table_name))
                try:
                    columns = data_struct[database_name][table_name]
                    columns_comma_sep = ",".join(columns)
                    cur.execute("SELECT {} FROM {}".format(columns_comma_sep,table_name))
                    result_table = PrettyTable(columns)
                    for col in result_table.align.keys():
                        result_table.align[col] = 'l'
                    for row in cur:
                        result_table.add_row([row[column_name] for column_name in columns])
                    result_string = result_table.get_string(print_empty=False)
                    if len(result_string) == 0:
                        printAndWriteToFile(r"{Empty table}")
                    else:
                        printAndWriteToFile(result_string)        
                except Exception as e:
                    print(r"{Failed to obtain}")
        connection.close()

def printAndWriteToFile(s,end="\n"):
    print(s,end=end)
    if outfile is not None:
        outfile.write(s + end)

# MAIN CODE

# Parse args
parser = ArgumentParser(description="Script that automates mssql database enumeration, and dumps all accessible data; written by CaptainWoof")
parser.add_argument("-u","--username",type=str,action='store',required=True,help="Username to use to login")
parser.add_argument("-p","--password",type=str,action='store',required=True,help="Password to use to login")
parser.add_argument("-H","--host",type=str,action='store',required=True,help="Host to connect to; default: localhost")
parser.add_argument("-P","--port",type=int,action='store',default=1433,required=False,help="Port to connect to; default: 1433")
parser.add_argument("-o","--output",type=str,action='store',required=False,help="File to output results to")

args = parser.parse_args()
username = args.username
password = args.password
host = args.host
port = args.port
outfile = open(args.output,'w+') if args.output is not None else None
data_struct = {}
conn,cursor = None,None

# Start enum
try:
    # Connect to database server
    conn,cursor = getConnCursor("master",print_banner=True)

    # Enum database names
    enumDatabases()

    # Enum tables
    enumTables()

    # Enum columns
    enumColumns()

    # Close db conn because individual connections would be needed now
    conn.close()

    # Dump data
    dumpData()
    
except Exception as e:
    print(e.__cause__())
finally:    
    # Close file stream, if open
    if outfile is not None:
        outfile.close()