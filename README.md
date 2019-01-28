# mybinlog_dig使用帮助：
该程序用于MySQL数据库的ROW格式binlog挖掘。它可用于查看日志或重新执行日志。

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
