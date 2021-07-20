#!/usr/bin/python3

from argparse import ArgumentParser
import pymssql
from prettytable import PrettyTable

def getConnCursor(print_banner=False):
    if print_banner:
        print("[>] Connecting to {} (port {})".format(host,port))
    conn = pymssql.connect(host=host,port=port,user=username,password=password)
    cursor = conn.cursor(as_dict=True)
    return conn,cursor

def enumLinkedServers():
    printAndWriteToFile("\n[+] Enumerating linked servers")
    cursor.execute("SELECT name,provider,data_source FROM sys.servers")
    results_table = PrettyTable(["Name","Provider","Data Source"])
    for row in cursor:
        results_table.add_row([row['name'],row['provider'],row['data_source']])
        servers_instances.append(row['name'])
    results_table.align['Name'] = 'l'
    results_table.align['Provider'] = 'l'
    results_table.align['Data Source'] = 'l'
    results_str = results_table.get_string(print_empty=False)
    if len(results_str) != 0:
        printAndWriteToFile(results_str)
    else:
        printAndWriteToFile(r"{None found}")

def enumDatabases(server_instance):
    # Connect to server
    printAndWriteToFile("\n[+] Enumerating database names")

    # Enumerate
    cursor.execute("SELECT name FROM [{}].master.dbo.sysdatabases".format(server_instance))
    printAndWriteToFile("Databases found: ",end="")
    for row in cursor:
        if row['name'] not in system_db and ignore_system_db:
            printAndWriteToFile(row['name'],end=", ")
            data_struct[row['name']] = None
        elif not ignore_system_db:
            printAndWriteToFile(row['name'],end=", ")
            data_struct[row['name']] = None
    printAndWriteToFile("")

def enumTables(server_instance):
    printAndWriteToFile("\n[+] Enumerating tables in each database")
    for i,database_name in enumerate(data_struct.keys()):
        try:
            printAndWriteToFile("#{}. {} => ".format(i,database_name),end="")
            # Enumerate
            cursor.execute("SELECT table_name FROM [{}].{}.information_schema.tables".format(server_instance,database_name))
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

def enumColumns(server_instance):
    printAndWriteToFile("\n[+] Enumerating column names of each table")
    for database_name in data_struct.keys():
        if data_struct[database_name] is not None:
            for table_name in data_struct[database_name].keys():
                try:
                    printAndWriteToFile("# {}.{} => ".format(database_name,table_name),end="")                    
                    cursor.execute("SELECT column_name FROM [{}].{}.information_schema.columns WHERE table_name='{}'".format(server_instance,database_name,table_name))
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

def dumpData(server_instance):
    printAndWriteToFile("\n[+] Dumping all accessible data")
    for database_name in data_struct.keys():        
        if data_struct[database_name] is not None:
            for table_name in data_struct[database_name].keys():
                print("\n{}.{}.{}:".format(server_instance,database_name,table_name))
                try:
                    columns = data_struct[database_name][table_name]
                    columns_comma_sep = ",".join(columns)
                    cursor.execute("SELECT {} FROM [{}].{}.dbo.{}".format(columns_comma_sep,server_instance,database_name,table_name))
                    result_table = PrettyTable(columns)
                    for col in result_table.align.keys():
                        result_table.align[col] = 'l'
                    for row in cursor:
                        result_table.add_row([row[column_name] for column_name in columns])
                    result_string = result_table.get_string(print_empty=False)
                    if len(result_string) == 0:
                        printAndWriteToFile(r"{Empty table}")
                    else:
                        printAndWriteToFile(result_string)        
                except Exception as e:
                    print(r"{Failed to obtain}")

def printAndWriteToFile(s,end="\n"):
    print(s,end=end)
    if outfile is not None:
        outfile.write(s + end)

# MAIN CODE

# Parse args
parser = ArgumentParser(description="Script that automates mssql database enumeration, and dumps all accessible data; written by CaptainWoof")
parser.add_argument("-u","--username",type=str,action='store',required=True,help="Username to use to login")
parser.add_argument("-p","--password",type=str,action='store',required=True,help="Password to use to login")
parser.add_argument("-H","--host",type=str,action='store',required=True,help="Host to connect to")
parser.add_argument("-L","--linked-server",type=str,action='store',required=False,default="",help="Comma-separated linked servers to enumerate; default: None (enumerate all linked servers); example: 'SERV\\DB01'")
parser.add_argument("-P","--port",type=int,action='store',default=1433,required=False,help="Port to connect to; default: 1433")
parser.add_argument("-o","--output",type=str,action='store',required=False,help="File to output results to")
parser.add_argument("-i","--ignore-system-db",action='store_true',required=False,help="Ignores system databases: master,model,msdb,tempdb;")

args = parser.parse_args()
username = args.username
password = args.password
host = args.host
linked_server = args.linked_server
port = args.port
outfile = open(args.output,'w+') if args.output is not None else None
servers_instances = []
data_struct = {}
conn,cursor = None,None
ignore_system_db = args.ignore_system_db
system_db = ["master","model","msdb","tempdb"]

# Start enum
try:
    # Connect to database server
    conn,cursor = getConnCursor(print_banner=True)

    # Enum linked servers
    if linked_server == '':
        enumLinkedServers()
    else:
        for ls in linked_server.split(","):
            servers_instances.append(ls)

    # For each found linked server, enum
    for server_instance in servers_instances:
        printAndWriteToFile("\n[#] Enumerating {}\n".format(server_instance) + '-'*30)
        enumDatabases(server_instance)
        enumTables(server_instance)
        enumColumns(server_instance)        
        dumpData(server_instance)
    
except Exception as e:
    print(e)
finally:
    conn.close()
    # Close file stream, if open
    if outfile is not None:
        outfile.close()
