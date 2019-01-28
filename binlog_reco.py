#!/usr/bin/python
#-*- coding: UTF-8 -*-
#author: lilongqian
#createDate: 2018-11-30
#modifyDate: 2019-01-11

import os
import re
import MySQLdb
import time
import datetime
import sys

#vv_db_addr=""
#vv_db_port=3306
#vv_db_socket=""
#vv_db_user=""
#vv_db_passwd=""
#vv_db_name=""
#vv_tab_name=""
#vv_tab_name=None

MYSQLBINLOG="/usr/local/mysql/bin/mysqlbinlog --base64-output=DECODE-ROWS -vv "

def usage():
    print '''
          --restore-type: for getting redo log or undo log. value: redo(default values) or undo 
          --paratype: method to parse datadict for finding column map relation. value: file or database.
          --parafile: the file for parsing datadict to find column map relation. 
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
          --time-delta: the time length of parsing binlog file every time(unit: second).
          --example:
              by database: 
                  python binlog_reco.py 
                  --restore-type=redo
                  --paratype=database 
                  --binlogfile="/{dbdatadir}/mysql-binlog.001833" 
                  --start-datetime='yyyy-mm-dd HH:MM:SS' ]
                  --stop-datetime='yyyy-mm-dd HH:MM:SS' ]
                  --host=database address ip 
                  --port=database port 
                  --socket=socket
                  --user=root 
                  --password=database user password 
                  --database=database name 
                  [ --table=tablename ]
                  [ --time-delta=xx (seconds) ]
              by file:
                  python binlog_reco.py
                  --paratype=file
                  --parafile=parafilename
                  --binlogfile="/{dbdatadir}/mysql-binlog.001833" 
                  --start-datetime='yyyy-mm-dd HH:MM:SS'
                  --stop-datetime='yyyy-mm-dd HH:MM:SS'
                  --database=database name
                  [--table=tablename]
                  [ --time-delta=xx (seconds) ]
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

    filecmd="cat " + in_filename
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
    else:
        print "database connection is error"
        sys.exit(0)

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

def get_sql_redo_statament(in_cmdline_list, in_tab_col_dict,in_db_name,in_table_name='', in_out_file='',in_flag=0):
    if len(in_cmdline_list) == 0:
        print "in_cmdline_list is null!"
        sys.exit(0)

    if len(in_tab_col_dict) == 0:
        print "in_tab_col_dict is null!"
        sys.exit(0)

    v_sql_str=''
    for eachLine in in_cmdline_list:
        v_sql_str+=re.sub(r'\s/\*.*\*/$',',',eachLine) #delete the binlog comments
    v_sql_list=v_sql_str.split("#####")

    #delete other sql statement where only getting the binlog files of a single table
    if len(in_table_name)>=1:
        for ii in range(len(v_sql_list)-1, -1, -1):
            if re.search(r'`' + in_db_name + '`\.`' + in_table_name +"`",v_sql_list[ii]) == None:
                 del v_sql_list[ii]  
    else:
        pass

    if in_flag>0: 
        vv_outfile=open(in_out_file, "a+") #attach
    else:
        vv_outfile=open(in_out_file, "w+") #overload

    v_tabname=None
    v_cnt=0
    for each_sql in v_sql_list:
        v_cnt=v_cnt+1
        v_tmp_sql=each_sql.strip()
        if len(v_tmp_sql) < 1:
            continue

        if re.match(r'^INSERT',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
        elif re.match(r'^UPDATE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[1].replace("`","").split(".")[1]
            v_tmp_sql="UPDATE " + v_tmp_sql.split()[1] + "\n" + v_tmp_sql[re.search(r'\bSET\b',v_tmp_sql).span()[0]:-1] + \
                       "\n" + re.sub( r',\n ','\n  AND',  v_tmp_sql[re.search(r'\bWHERE\b', v_tmp_sql).span()[0] : re.search(r'\bSET\b', v_tmp_sql).span()[0] -2 ] ) + ";"
        elif re.match(r'^DELETE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
        elif re.match(r'^CREATE',v_tmp_sql) or re.match(r'^DROP',v_tmp_sql) or re.match(r'^ALTER',v_tmp_sql) or re.match(r'^TRUNCATE',v_tmp_sql):
            vv_outfile.writelines( "#" + "-" * 100 + "\n" )
            vv_outfile.writelines( v_tmp_sql.strip() + ";" + "\n" ) 
            continue
        else:
            continue
         
        v_tabcol_dict=in_tab_col_dict[v_tabname]
        for ii in v_tabcol_dict:
            v_tmp_sql=v_tmp_sql.replace(ii+"=", v_tabcol_dict[ii]+"=")

        vv_outfile.writelines("#" + "-" * 100 + "\n")
        vv_outfile.writelines(v_tmp_sql.strip()[:-1] + ";" + "\n")
        if v_cnt % 1000 == 0:
            vv_outfile.flush()

    vv_outfile.close()
    return 0

def get_sql_undo_statament(in_cmdline_undostring, in_tab_col_dict, in_db_name,in_table_name='', in_out_file='',in_flag=0):
    if len(in_cmdline_undostring) == 0:
        print "in_cmdline_undostring is null!"
        sys.exit(0)

    if len(in_tab_col_dict) == 0:
        print "in_tab_col_dict is null!"
        sys.exit(0)

    v_sql_str=''
    for eachLine in in_cmdline_undostring:
        v_sql_str+=re.sub(r'\s/\*.*\*/$',',',eachLine) #delete the binlog comments
    v_sql_list=v_sql_str.split("#####") #delete other sql statement where only getting the binlog files of a single table if len(in_table_name)>=1:

    if len(in_table_name)>=1:
        for ii in range(len(v_sql_list)-1, -1, -1):
            if re.search(r'`' + in_db_name + '`\.`' + in_table_name +"`",v_sql_list[ii]) == None:
                 del v_sql_list[ii]
    else:
        pass

    if in_flag>0:
        vv_outfile=open(in_out_file, "a+") #attach
    else:
        vv_outfile=open(in_out_file, "w+") #overload

    v_tabname=None
    v_cnt=0

    print "get_sql_undo_statament-->len(v_sql_list)==",len(v_sql_list) 
    for ii in range(len(v_sql_list)-1,-1,-1):
        v_cnt=v_cnt+1
        v_tmp_sql=v_sql_list[ii].strip()
        if len(v_tmp_sql) < 1:
            continue

        if re.match(r'^INSERT',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
            v_tmp_sql="DELETE FROM " + v_tmp_sql.split()[2] + "\nWHERE" + re.sub(r',\n','\nAND',v_tmp_sql[re.search(r'\bSET\b',v_tmp_sql).span()[0]+4::])          
        elif re.match(r'^UPDATE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[1].replace("`","").split(".")[1]
            v_tmp_sql="UPDATE " + v_tmp_sql.split()[1] + "\nSET\n" + v_tmp_sql[re.search(r'\bWHERE\b',v_tmp_sql).span()[1]+1 : re.search(r'\bSET\b',v_tmp_sql).span()[0] - 2 ] + \
                      "\nWHERE\n" + re.sub( r',\n ','\n  AND', v_tmp_sql[re.search(r'\bSET\b',v_tmp_sql).span()[1]+1 ::] )
        elif re.match(r'^DELETE',v_tmp_sql):
            v_tabname=v_tmp_sql.split()[2].replace("`","").split(".")[1]
            v_tmp_sql="INSERT INTO " + v_tmp_sql.split()[2] + \
                      "\nSET\n" + \
                      v_tmp_sql[re.search(r'\bWHERE\b',v_tmp_sql).span()[1]+1 ::]
        elif re.match(r'^CREATE',v_tmp_sql) or re.match(r'^DROP',v_tmp_sql) or re.match(r'^ALTER',v_tmp_sql) or re.match(r'^TRUNCATE',v_tmp_sql):
            vv_outfile.writelines( "#" + "-" * 100 + "\n" )
            vv_outfile.writelines( v_tmp_sql.strip() + ";" + "\n" ) 
            continue
        else:
            continue

        v_tabcol_dict=in_tab_col_dict[v_tabname]
        for ii in v_tabcol_dict:
            v_tmp_sql=v_tmp_sql.replace(ii+"=", v_tabcol_dict[ii]+"=")

        vv_outfile.writelines("#" + "-" * 100 + "\n")
        vv_outfile.writelines(v_tmp_sql.strip()[:-1] + ";" + "\n")
        if v_cnt % 1000 == 0:
            vv_outfile.flush()

    vv_outfile.close()
    return 0

def get_start_datetime(in_binlog_file):
    #get start datetime of binlogfile 
    v_strtime=os.popen("mysqlbinlog --stop-position=200 " + in_binlog_file + " 2>&1 " + "|grep -e '#[0-9]\{6\}'|head -1|cut -c 2-16").read(15)
    v_start_datetime=v_start_datetime="20"+v_strtime[0:2]+"-"+v_strtime[2:4]+"-"+v_strtime[4:6]+v_strtime[6:] 
    return v_start_datetime

def get_stop_datetime(in_binlog_file):
    #get stop datetime of binlogfile 
    v_end_datetime=os.popen("stat " + in_binlog_file + "|grep 'Change'|awk -F': ' '{print $2}'|awk -F'.' '{print $1}'").readline()
    return v_end_datetime.strip()

def call_redo_sql(in_tab_col_dict, in_db_name, in_start_datetime, in_stop_datetime, in_out_file, in_step_delta):
    global MYSQLBINLOG #global mysqlbinlog variables
    vv_sql_tmpfile="" 

    if len(in_db_name) >= 1:
        BINLOG_CMD=MYSQLBINLOG + " --database=" + in_db_name 
    else:
        BINLOG_CMD=MYSQLBINLOG

    vv_start_ptr=in_start_datetime
    vv_stop_ptr=(datetime.datetime.strptime(vv_start_ptr,'%Y-%m-%d %H:%M:%S') + in_step_delta).strftime('%Y-%m-%d %H:%M:%S')
    call_cnt=0

    while vv_start_ptr < in_stop_datetime:
        print "-" * 100
        if vv_stop_ptr > in_stop_datetime:
            vv_stop_ptr = in_stop_datetime 

        ###############################################################################################################################
        #Set mysqlbinlog cmdline parameter
        ###############################################################################################################################
        BINLOG_STMT=BINLOG_CMD + " --start-datetime='" + vv_start_ptr +"'" + " --stop-datetime='" + vv_stop_ptr + "'"
        
        ###############################################################################################################################
        #Create tmpfile for saving statements decoded from binlog file 
        ###############################################################################################################################
        #vv_tmpdir=os.path.dirname(vv_out_file)
        vv_tmpdir=r"/tmp"
        vv_sql_tmpfile=vv_tmpdir+ r"/" + r"tmp_binlog2redo.dump"
        if os.path.exists(vv_sql_tmpfile):
            os.remove(vv_sql_tmpfile)

        print "%s:Begin to decode binlog file" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        print BINLOG_STMT + " " + vv_logfile + " > " + vv_sql_tmpfile
        iret=os.system(BINLOG_STMT + " " + vv_logfile + " > " + vv_sql_tmpfile) 
        if iret !=0:
            print "Decode binlogfile " + vv_logfile + " failure!"
            sys.exit(0)

        ###############################################################################################################################
        #Get the sql statements by shell commands(grep and sed) and save them in the vv_cmdline_list variable 
        ###############################################################################################################################
        print "%s:Load parsed binlog file to memory " %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        filecmd=r"cat " + vv_sql_tmpfile + r"|grep '^###'|sed 's/^### //;s/\(^.*=\).* (\(.*\))/\1\2/'|sed '/^INSERT\|^UPDATE\|^DELETE\|^CREATE\|^DROP\|^ALTER\|^TRUNCATE/i #####'"
        vv_cmdline_list=os.popen(filecmd).readlines()
        if len(vv_cmdline_list)==0:
            vv_start_ptr=(datetime.datetime.strptime(vv_stop_ptr,'%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            vv_stop_ptr=(datetime.datetime.strptime(vv_stop_ptr,'%Y-%m-%d %H:%M:%S') + vv_step_delta).strftime('%Y-%m-%d %H:%M:%S')
            call_cnt+=1
            print "call_cnt=%d" %(call_cnt)
            continue
        
        ###############################################################################################################################
        #Get the complete sql statements by get_sql_statament function
        ###############################################################################################################################
        print "%s:Begin to generate sql statement from parsed binlog file!" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        iret=get_sql_redo_statament(vv_cmdline_list, in_tab_col_dict, in_db_name, vv_tab_name, in_out_file, call_cnt)
        if iret != 0:
            print "Running function get_sql_statament failure!"
        print "%s:Finished generating sql statement from parsed binlog file!" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))

        if os.path.exists(vv_sql_tmpfile):
            print "clear tmp files"
            with open(vv_sql_tmpfile,"w") as myfile:
                 myfile.truncate()

        vv_start_ptr=(datetime.datetime.strptime(vv_stop_ptr,'%Y-%m-%d %H:%M:%S') + datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
        vv_stop_ptr=(datetime.datetime.strptime(vv_stop_ptr,'%Y-%m-%d %H:%M:%S') + vv_step_delta).strftime('%Y-%m-%d %H:%M:%S')
        call_cnt+=1
        print "call_cnt=%d" %(call_cnt)

    if os.path.exists(vv_sql_tmpfile):
        os.remove(vv_sql_tmpfile)

    return 0    

def call_undo_sql(in_tab_col_dict, in_db_name, in_start_datetime, in_stop_datetime, in_out_file, in_step_delta):
    global MYSQLBINLOG #global mysqlbinlog variables
    vv_sql_tmpfile="" 

    if len(in_db_name) >= 1:
        BINLOG_CMD=MYSQLBINLOG + " --database=" + in_db_name 
    else:
        BINLOG_CMD=MYSQLBINLOG

    vv_stop_ptr=in_stop_datetime
    vv_start_ptr=(datetime.datetime.strptime(vv_stop_ptr,'%Y-%m-%d %H:%M:%S') - in_step_delta).strftime('%Y-%m-%d %H:%M:%S')
    call_cnt=0

    while vv_stop_ptr > in_start_datetime:
        print "-" * 100
        if vv_start_ptr < in_start_datetime:
            vv_start_ptr = in_start_datetime 

        ###############################################################################################################################
        #Set mysqlbinlog cmdline parameter
        ###############################################################################################################################
        BINLOG_STMT=BINLOG_CMD + " --start-datetime='" + vv_start_ptr +"'" + " --stop-datetime='" + vv_stop_ptr + "'"
        
        ###############################################################################################################################
        #Create tmpfile for saving statements decoded from binlog file 
        ###############################################################################################################################
        #vv_tmpdir=os.path.dirname(vv_out_file)
        vv_tmpdir=r"/tmp"
        vv_sql_tmpfile=vv_tmpdir+ r"/" + r"tmp_binlog2undo.dump"
        #vv_sql_oldtmpfile=vv_tmpdir+ r"/" + r"tmp_binlog2undo_" +time.strftime(r'%H%M%S',time.localtime()) + r".dump"
        if os.path.exists(vv_sql_tmpfile):
            os.remove(vv_sql_tmpfile)

        print "%s:call_undo_sql-->Begin to decode binlog file for undo statement" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        print BINLOG_STMT + " " + vv_logfile + " > " + vv_sql_tmpfile
        iret=os.system(BINLOG_STMT + " " + vv_logfile + " > " + vv_sql_tmpfile) 
        if iret !=0:
            print "call_undo_sql-->Decode binlogfile " + vv_logfile + " failure!"
            sys.exit(0)

        ###############################################################################################################################
        #Get the sql statements by shell commands(grep and sed) and save them in the vv_cmdline_undostring variable 
        ###############################################################################################################################
        print "%s:call_undo_sql-->Load parsed binlog file to memory " %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        filecmd=r"cat " + vv_sql_tmpfile + r"|grep '^###'|sed 's/^### //;s/\(^.*=\).* (\(.*\))/\1\2/'|sed '/^INSERT\|^UPDATE\|^DELETE\|^CREATE\|^DROP\|^ALTER\|^TRUNCATE/i #####'"
        vv_cmdline_undostring=os.popen(filecmd).readlines()
        if len(vv_cmdline_undostring)==0:
            vv_stop_ptr=(datetime.datetime.strptime(vv_start_ptr,'%Y-%m-%d %H:%M:%S') - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            vv_start_ptr=(datetime.datetime.strptime(vv_start_ptr,'%Y-%m-%d %H:%M:%S') - vv_step_delta).strftime('%Y-%m-%d %H:%M:%S')
            call_cnt+=1
            print "call_undo_sql-->call_cnt=%d" %(call_cnt)
            continue

        ###############################################################################################################################
        #Get the complete sql statements by get_sql_statament function
        ###############################################################################################################################
        print "%s:call_undo_sql-->Begin to generate undo sql statement from parsed binlog file!" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))
        iret=get_sql_undo_statament(vv_cmdline_undostring, in_tab_col_dict, in_db_name, vv_tab_name, in_out_file, call_cnt)
        if iret != 0:
            print "call_undo_sql-->Running function get_sql_undo_statament failure!"
            return -1
        print "%s:call_undo_sql-->Finished generating undo sql statement from parsed binlog file!" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))

        if os.path.exists(vv_sql_tmpfile):
            print "clear tmp files"
            with open(vv_sql_tmpfile,"w") as myfile:
                 myfile.truncate()

        vv_stop_ptr=(datetime.datetime.strptime(vv_start_ptr,'%Y-%m-%d %H:%M:%S') - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
        vv_start_ptr=(datetime.datetime.strptime(vv_start_ptr,'%Y-%m-%d %H:%M:%S') - vv_step_delta).strftime('%Y-%m-%d %H:%M:%S')
        call_cnt+=1
        print "call_undo_sql-->call_cnt=%d" %(call_cnt)

    if os.path.exists(vv_sql_tmpfile):
        os.remove(vv_sql_tmpfile)

    return 0    
if __name__=="__main__":
    import getopt
    reload(sys)
    sys.setdefaultencoding("utf8") #set charset for chinese 

    ###################################################################################################################
    #Initilize input variables
    ###################################################################################################################
    vv_restore_type="" #
    vv_paratype="" #file or database
    vv_binlog_file="" #
    vv_start_datetime="" #
    vv_stop_datetime="" #
    vv_para_file="" #
    vv_db_addr="" #
    vv_db_port=3306 #3306
    vv_db_socket="" #
    vv_db_user="" #
    vv_db_passwd="" #
    vv_db_name="" #
    vv_tab_name="" #
    vv_out_file="" #
    vv_time_delta="" #

    ###################################################################################################################
    #Parse command line parameter
    ###################################################################################################################
    (myopts,myargs)=getopt.getopt(sys.argv[1:],"hv",["help","restore-type=","paratype=","parafile=","binlogfile=","start-datetime=",\
                                  "stop-datetime=","parafile=","host=","port=","socket=","user=","password=","database=","table=","outfile=","time-delta="])
    if len(myopts) <= 0:
        print "Please input correct parameter!"
        sys.exit(0)

    for pp in myopts:
        if pp[0]=="--help" or pp[0]=="-h":
            usage()
            sys.exit(0)

        if pp[0]=="--restore-type":
            vv_restore_type=pp[1]
        elif pp[0]=="--paratype":
            vv_paratype=pp[1] 
            if vv_paratype not in ("file","database"):
                print "please input paratype=file or paratype=database"
                sys.exit(0)
        elif pp[0]=="--binlogfile":
            vv_logfile=pp[1] 
            if len(vv_logfile)==0:
                print "please input binlog file"
                sys.exit(0)
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
        elif pp[0]=="--outfile":
            vv_out_file=pp[1]
        elif pp[0]=="--time-delta":
            vv_time_delta=pp[1]
        else:
            continue 

    if len(vv_out_file) < 5:
        print "--outfile must be given!"
        sys.exit(0)

    if vv_restore_type not in [ "redo","undo" ]:
        print "restore type have automatically set to redo type"
        vv_restore_type="redo"

    ###############################################################################################################################
    #Parse cmdline parameter and save them in local variables
    ###############################################################################################################################
    print "vv_paratype=",vv_paratype
    vv_tab_col_dict=None
    if vv_paratype=="file":
        print "get column metadata by file"
        print "The input parameters are:"
        print "restore-type: %s" %(vv_restore_type) 
        print "paratype: %s" %(vv_paratype) 
        print "parafile: %s" %(vv_para_file) 
        print "binlogfile: %s" %(vv_logfile) 
        print "start-datetime: %s" %(vv_start_datetime) 
        print "stop-datetime: %s" %(vv_stop_datetime) 
        print "outfile: %s" %(vv_out_file) 
        vv_tab_col_dict=fetch_table_columns_by_file(vv_para_file)
        if len(vv_tab_col_dict)==0:
            print "vv_tab_col_dict is null[fetch_table_columns_by_file]"
            sys.exit(0)
    elif vv_paratype=="database":
        print "get column metadata by database"
        print "The input parameters are:"
        print "restore-type: %s" %(vv_restore_type) 
        print "paratype: %s" %(vv_paratype) 
        print "binlogfile: %s" %(vv_logfile) 
        print "start-datetime: %s" %(vv_start_datetime) 
        print "stop-datetime: %s" %(vv_stop_datetime) 
        print "host: %s" %(vv_db_addr) 
        print "port: %s" %(vv_db_port) 
        print "socket: %s" %(vv_db_socket) 
        print "user: %s" %(vv_db_user) 
        print "password: xxxxxxxxx" 
        print "database: %s" %(vv_db_name) 
        print "table: %s" %(vv_tab_name) 
        print "outfile: %s" %(vv_out_file) 
        vv_tab_col_dict=fetch_table_columns_by_db(vv_db_addr,vv_db_port,vv_db_socket,vv_db_user,vv_db_passwd,vv_db_name,vv_tab_name)
        if len(vv_tab_col_dict)==0:
            print "vv_tab_col_dict is null[fetch_table_columns_by_db]"
            sys.exit(0)
    
    ###############################################################################################################################
    #if vv_start_datetime is null then geting start_datetime and end_datetime from binlog file.
    ###############################################################################################################################
    if len(vv_start_datetime)< 19:
        vv_start_datetime=get_start_datetime(vv_logfile)
    if len(vv_stop_datetime)< 19:
        vv_stop_datetime=get_stop_datetime(vv_logfile)

    if len(vv_time_delta) > 0:
        vv_step_delta=datetime.timedelta(seconds=int(vv_time_delta))
    else:
        vv_step_delta=datetime.timedelta(seconds=200)

    if vv_restore_type=="redo":
        iret=call_redo_sql(vv_tab_col_dict, vv_db_name, vv_start_datetime, vv_stop_datetime, vv_out_file, vv_step_delta)
        if iret!=0:
            print "Running call_redo_sql function failure!"
    elif vv_restore_type=="undo":
        iret=call_undo_sql(vv_tab_col_dict, vv_db_name, vv_start_datetime, vv_stop_datetime, vv_out_file, vv_step_delta)
        if iret!=0:
            print "Running call_undo_sql function failure!"
    else:
        pass

    print "please check outfile: %s" %(vv_out_file)
    print "%s:Binlog parsing finished!" %(time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()))

