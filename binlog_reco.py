#!/usr/bin/python
#-*- coding: UTF-8 -*-
#author: lilongqian
#date: 2018-11-30

import os
import re
import MySQLdb
import time
import sys


#vv_db_addr="localhost"
#vv_db_port=3306
#vv_db_socket="/home/mysql_data/mysql.sock"
#vv_db_user="root"
#vv_db_passwd="CA#KF11D85SAR#!"
#vv_db_name="mongo"
#vv_tab_name=""
#vv_tab_name=None

def usage():
    print '''
          --paratype: method to parse datadict for finding column map relation. value:file or database.
          --parafile: the file for parseing datadict to find column map relation 
          --binlogfile: mysql database binlog file.
          --start-datetime: timestamp to begin decode binlog file.
          --stop-datetime: timestamp to end decode binlog file.
          --host: the host address that database instance belongs to.
          --port: the service port of the database instance.
          --socket: the socket file of the database.
          --database: the database to the binlog file to be decode.
          --user: the user connecting to database that generates binlog file.
          --password: the password of the user connecting to database.
          --table: the table to be decode from binlog file.
          --example:
              by database: 
                  python binlog_reco.py 
                  --paratype=database 
                  --binlogfile="/{dbdatadir}/mysql-binlog.001833" 
                  [--start-datetime='yyyy-mm-dd HH:MM:SS' ]
                  [--stop-datetime='yyyy-mm-dd HH:MM:SS' ]
                  --host=database address ip 
                  --port=database port 
                  --socket=socket
                  --user=root 
                  --password=database user password 
                  --database=database name 
                  [ --table=tablename ]
              by file:
                  python binlog_reco.py
                  --paratype=file
                  --parafile=parafilename
                  --binlogfile="/{dbdatadir}/mysql-binlog.001833" 
                  --start-datetime='yyyy-mm-dd HH:MM:SS'
                  --stop-datetime='yyyy-mm-dd HH:MM:SS'
                  --database=database name
                  [--table=tablename]
          '''

def fetch_table_columns_by_file(in_filename):
    '''
    get table columns by the following SQL:
        SELECT 
          CONCAT_WS('#', table_name,  CONCAT('@', ordinal_position), column_name)
        FROM information_schema.columns 
        WHERE table_schema = database name #[ and table_name = table name]
        ORDER BY table_name,  ordinal_position 
    format: delete_base#@1#id [seperator:#]
    '''

    #firstrowcmd="head -1 " + in_filename
    filecmd="cat " + in_filename
    
    #fistrow=os.popen(firstrowcmd).readlines()[0]
    mylist=os.popen(filecmd).readlines()
       
    gv_table_dict = dict()
    gv_column_dict = dict()

    v_tabname_ptr=""
    v_tabname_ref=""

    iii=0
    for linename in mylist:
        iii=iii+1
        linename=linename.strip()
        if len(linename) < 1:
            continue

        v_tabname_ptr = linename.split("#")[0]
        v_col_id = linename.split("#")[1]
        v_col_name = linename.split("#")[2]

        #print "***{%s--%s--%s--%s}***" %(v_tabname_ref,v_tabname_ptr,v_col_id,v_col_name)
        if v_tabname_ref != v_tabname_ptr and len(v_tabname_ref) >=1 :
            gv_table_dict[v_tabname_ref] = gv_column_dict
            gv_column_dict = dict()
        elif len(v_tabname_ref) < 1 and len(v_tabname_ptr) >=1 : 
            gv_table_dict[v_tabname_ptr] = gv_column_dict
        else:
            pass

        gv_column_dict[v_col_id]=v_col_name
        v_tabname_ref=v_tabname_ptr
        #print "gv_table_dict[", v_tabname_ptr, "]=", gv_table_dict[ v_tabname_ptr ]

    gv_table_dict[v_tabname_ptr] = gv_column_dict

    time.sleep(2)
    return gv_table_dict
    

def fetch_table_columns_by_db(in_db_addr="",in_db_port=3306,in_db_socket="",in_db_user="",in_db_passwd="",in_db_name="",in_tab_name=""):

    if len(in_db_socket) >0 :
        dbconn=MySQLdb.connect(host=in_db_addr,unix_socket=in_db_socket,user=in_db_user,passwd=in_db_passwd,db=in_db_name,charset='utf8',connect_timeout=100)
    else:
        dbconn=MySQLdb.connect(host=in_db_addr,port=in_db_port,user=in_db_user,passwd=in_db_passwd,db=in_db_name,charset='utf8',connect_timeout=100)
    cfgCursor=dbconn.cursor()

    if len(in_db_name) > 0 and len(in_tab_name) > 0 :  
        vsql = " SELECT table_name, CONCAT('@',ordinal_position) AS ordinal_position,column_name FROM information_schema.columns " \
                                 + " WHERE table_schema = '" + in_db_name + "' and table_name = '" + in_tab_name + "'" + " order by table_name, ordinal_position "
    elif len(in_db_name) > 0 and len(in_tab_name) == 0 :
        vsql = " SELECT table_name, CONCAT('@',ordinal_position) AS ordinal_position,column_name FROM information_schema.columns " \
                                 + " WHERE table_schema = '" + in_db_name + "'" + " order by table_name, ordinal_position " 
    retcnt=cfgCursor.execute(vsql)
    vv_result=cfgCursor.fetchall()
    
    gv_table_dict = dict()
    gv_column_dict = dict()
    v_tabname_ptr = ""
    v_tabname_ref = ""

    for ii in vv_result:
        v_tabname_ptr = ii[0]
        v_col_id = ii[1]
        v_col_name = ii[2]

        if v_tabname_ref != v_tabname_ptr and len(v_tabname_ref) > 0 :
            gv_table_dict[v_tabname_ref] = gv_column_dict
            gv_column_dict = dict()
        elif len(v_tabname_ref) < 1 and len(v_tabname_ptr) > 0:
            gv_table_dict[v_tabname_ptr] = gv_column_dict
        else:
            pass

        gv_column_dict[v_col_id] = v_col_name
        v_tabname_ref=v_tabname_ptr

    gv_table_dict[v_tabname_ptr] = gv_column_dict
    return gv_table_dict

def get_sql_statament(in_cmdline_list, in_col_dict,in_db_name,in_table_name=''):
    if len(in_cmdline_list) == 0 or len(in_col_dict)==0:
        print "in_cmdline_list or in_col_dict are not null"
        sys.exit(0)

    v_sql_str=''
    for eachLine in in_cmdline_list:
        v_sql_str+=re.sub(r'\s/\*.*\*/$',',',eachLine) #delete the binlog comments
    v_sql_list=v_sql_str.split("#####")

    if len(in_table_name)>=1:
        for ii in range(len(v_sql_list)-1, -1, -1):
            if re.search(r'`' + in_db_name + '`\.`' + in_table_name +"`",v_sql_list[ii]) == None:
                 del v_sql_list[ii]  
    else:
        pass
    
    v_tabname=None
    for each_sql in v_sql_list:
        v_tmp_sql=each_sql.strip()
        if len(v_tmp_sql) < 1:
            continue

        if re.match(r'^INSERT',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
        elif re.match(r'^UPDATE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[1].replace("`","").split(".")[1]
        elif re.match(r'^DELETE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
        elif re.match(r'^CREATE',v_tmp_sql) or re.match(r'^DROP',v_tmp_sql) or re.match(r'^ALTER',v_tmp_sql) or re.match(r'^TRUNCATE',v_tmp_sql):
            print "#" + "-" * 100
            print v_tmp_sql.strip() + ";" 
            continue
        else:
            continue
         
        v_tabcol_dict=v_tab_col_dict[v_tabname]
        for ii in v_tabcol_dict:
            v_tmp_sql=v_tmp_sql.replace(ii,v_tabcol_dict[ii])
        print "#" + "-" * 100
        print v_tmp_sql.strip()[:-1] + ";"
        print "commit;"

    return 0

if __name__=="__main__":
    import getopt
    reload(sys)
    sys.setdefaultencoding("utf8") 
    #######################################################################################################33
    #parse command line parameter
    #######################################################################################################33
    vv_paratype="" #
    vv_binlog_file="" # 
    vv_start_datetime="" #
    vv_stop_datetime="" #
    vv_para_file="" #
    vv_db_addr="" #
    vv_db_port=3306 #
    vv_db_socket="" #
    vv_db_user="" #
    vv_db_passwd="" #
    vv_db_name="" #
    vv_tab_name="" #

    (myopts,myargs)=getopt.getopt(sys.argv[1:],"hv",["help","paratype=","parafile=","binlogfile=","start-datetime=","stop-datetime=","parafile=","host=","port=","socket=","user=","password=","database=","table="])
    if len(myopts) <= 0:
        print "Please input correct parameter!"
        sys.exit(0)

    for pp in myopts:
        if pp[0]=="--help" or pp[0]=="-h":
            usage()
            sys.exit(0)
        if pp[0]=="--paratype" :
            vv_paratype=pp[1] 
            if vv_paratype not in ("file","database"):
                print "please input paratype=file or paratype=database"
                sys.exit(0)
        elif pp[0]=="--binlogfile":
            vv_logfile=pp[1] 
        elif pp[0]=="--start-datetime":
            vv_start_datetime=pp[1] 
        elif pp[0]=="--stop-datetime":
            vv_stop_datetime=pp[1] 
        elif pp[0]=="--parafile":
            vv_para_file=pp[1]
        elif pp[0]=="--host":
            vv_db_addr=pp[1]
        elif pp[0]=="--port":
            vv_db_port=pp[1]
        elif pp[0]=="--socket":
            vv_db_socket=pp[1]
        elif pp[0]=="--user":
            vv_db_user=pp[1]
        elif pp[0]=="--password":
            vv_db_passwd=pp[1]
        elif pp[0]=="--database":
            vv_db_name=pp[1]
        elif pp[0]=="--table":
            vv_tab_name=pp[1]
        else:
            continue 

    print "vv_paratype=",vv_paratype
    v_tab_col_dict=None
    if vv_paratype=="file":
        print "get column metadata by file"
        print "The input parameters are:"
        print "paratype: %s" %(vv_paratype) 
        print "parafile: %s" %(vv_para_file) 
        print "binlogfile: %s" %(vv_logfile) 
        print "start-datetime: %s" %(vv_start_datetime) 
        print "stop-datetime: %s" %(vv_stop_datetime) 
        v_tab_col_dict=fetch_table_columns_by_file(vv_para_file)
    elif vv_paratype=="database":
        print "get column metadata by database"
        print "The input parameters are:"
        print "paratype: %s" %(vv_paratype) 
        print "binlogfile: %s" %(vv_logfile) 
        print "start-datetime: %s" %(vv_start_datetime) 
        print "stop-datetime: %s" %(vv_stop_datetime) 
        print "host: %s" %(vv_db_addr) 
        print "port: %s" %(vv_db_port) 
        print "socket: %s" %(vv_db_socket) 
        print "user: %s" %(vv_db_user) 
        print "password: %s" %(vv_db_passwd) 
        print "database: %s" %(vv_db_name) 
        print "table: %s" %(vv_tab_name) 
        v_tab_col_dict=fetch_table_columns_by_db(vv_db_addr,vv_db_port,vv_db_socket,vv_db_user,vv_db_passwd,vv_db_name,vv_tab_name)

    
    MYSQLBINLOG="/usr/local/mysql/bin/mysqlbinlog --base64-output=DECODE-ROWS -vv "
    if len(vv_db_name) >= 1:
        MYSQLBINLOG=MYSQLBINLOG + " --database=" + vv_db_name
    if len(vv_start_datetime) >= 19:
        MYSQLBINLOG=MYSQLBINLOG + " --start-datetime='" + vv_start_datetime +"'" 
    if len(vv_stop_datetime) >= 19:
        MYSQLBINLOG=MYSQLBINLOG + " --stop-datetime='" + vv_stop_datetime + "'"

    vv_workdir=os.getcwd()
    vv_sql_dumpfile=vv_workdir+"/" + "binlogdump_tmp.dump"
    print MYSQLBINLOG + " " + vv_logfile + " > " + vv_sql_dumpfile
    iret=os.system(MYSQLBINLOG + " " + vv_logfile + " > " + vv_sql_dumpfile) 
    if iret !=0:
        print "Decode binlogfile " + vv_logfile + " failure!"
        sys.exit(0)


    filecmd="cat " + vv_sql_dumpfile + "|grep '^###'|sed 's/^### //'|sed '/^INSERT\|^UPDATE\|^DELETE\|^CREATE\|^DROP\|^ALTER\|^TRUNCATE/i #####'"
    vv_cmdline_list=os.popen(filecmd).readlines()

    iret=get_sql_statament(vv_cmdline_list,v_tab_col_dict,vv_db_name,vv_tab_name)
    if iret != 0:
        print "Running function get_sql_statament failure!"

    sys.exit(0)
    if os.path.exists(vv_sql_dumpfile):
        os.remove(vv_sql_dumpfile)

    print "finished!"
