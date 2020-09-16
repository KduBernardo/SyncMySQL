import mysql.connector
import logging
from mysql.connector import Error
from configparser import ConfigParser
import datetime
#configs
config = ConfigParser()
config.read('conf')
#-----------------DataBase---------------------
#---origin
dbhost = config.get('database', 'host')
dbport = config.get('database', 'port')
dbuser = config.get('database', 'user')
dbpassword = config.get('database', 'pass')
dbname = config.get('database', 'name')
#---destin
dbhostd = config.get('databased', 'host')
dbportd = config.get('databased', 'port')
dbuserd = config.get('databased', 'user')
dbpasswordd = config.get('databased', 'pass')
dbnamed = config.get('databased', 'name')
#----------------logging-----------------------
logEnable = config.get('logging', 'logEnable')
pathLog = config.get('logging', 'fileLog')
logging.basicConfig(filename=pathLog,level=logging.INFO)
logging.basicConfig(filename=pathLog,level=logging.ERROR)
#---------------global var--------------------
listcommand = []


def synchronize():
    global listcommand
    logging.info('connecting in database 1 ...')
    reg1 = sqlfunction('origin', 'select', {'tables': 'updateSync', 'fields': '*', 'options': 'order by updateSync_data'})
    if reg1[1] == 0:
        logging.error('fail to connect in database 1: %s' % str(reg1[0]))
        return str(reg1[0])
    logging.info('connecting in database 1 ...')
    reg2 = sqlfunction('destin', 'select', {'tables': 'updateSync', 'fields': '*', 'options': 'order by updateSync_data'})
    if reg2[1] == 0:
        logging.error('fail to connect in database 2: %s' % str(reg2[0]))
        return str(reg2[0])
    for i in reg1[0]:
            i=i+('origin',)
            ordener(i)
    for i in reg2[0]:
            i=i+('destin',)
            ordener(i)
    for i in listcommand:
        strsql = (str(i[1].decode('utf8')))
        resp = sqlfunction('origin','free',{'sql': strsql})
        if resp[1] == 0:
            solveErrors(i, resp[0],'origin')
        resp = sqlfunction('destin', 'free', {'sql': strsql})
        if resp[1] == 0:
            solveErrors(i, resp[0],'destin')
    clear = sqlfunction('origin','delete',{'tables':'updateSync'})
    logging.info(clear)
    clear = sqlfunction('destin', 'delete', {'tables': 'updateSync'})
    logging.info(clear)
    return 0

def solveErrors(obj, err,db):
    strsql = (str(obj[1].decode('utf8')))
    print(strsql)
    if 'Duplicate entry' in err:
        if db == obj[4]: return 0
        table = strsql.split(' into ')[1].split('(')[0]
        primaryKey = getPK(table)
        strshow = strsql + ' ON DUPLICATE KEY UPDATE %s=%s+1' %(primaryKey,primaryKey)
        resp = sqlfunction(db, 'free', {'sql': strshow})
    return resp

#this method can be use the data base to get primary keys, just implements :)
def getPK(t):
    pk = config.get('pk', t)
    return pk



def ordener(p):
    flag = 0
    res = []
    global listcommand
    if len(listcommand) == 0:
        listcommand.append(p)
        return 0
    for i in listcommand:
        if flag == 0:
            if i[2] <= p[2]:
                res.append(i)
            else:
                res.append(p)
                res.append(i)
                flag = 1
        else:
            res.append(i)
    if len(res) <= len(listcommand): res.append(p)
    listcommand = res
    return 0



def sqlfunction(db, dmltype, args):
    con = connectDB(db)
    if con == 0: return ['not connected',0]
    #get atributes
    tables = ''
    conditions = ''
    fields = ''
    values = ''
    op = ''
    if 'tables' in args:
        tables = args['tables']
    if 'conditions' in args:
        conditions = ' where '+str(args['conditions'])
    if 'fields' in args:
        fields = str(args['fields']).replace('[', '(').replace(']', ')').replace("'", "")
    if 'values' in args:
        values = str(args['values']).replace('[', '(').replace(']', ')')
    if 'options' in args:
        op = str(args['options'])
    #select type
    if dmltype == 'free':
        strSQL = args['sql']
    if dmltype == 'insert':
        strSQL = 'insert into %s %s values %s;' % (tables, fields, values)
    if dmltype == 'update':
        strSQL='update %s set %s %s'%(tables,values,conditions)
    if dmltype == 'delete':
        strSQL='delete from %s %s' %(tables,conditions)
    if dmltype == 'select':
        strSQL='select %s from %s %s %s;' %(fields,tables,conditions,op)
    cursor = con.cursor()
    try:
        cursor.execute(strSQL)

        if dmltype == 'select':
            rec = cursor.fetchall()
        else:
            rec = 'success'
        con.commit()
    except Error as e:
        return [str(e),0]

    return [rec, 1]


def connectDB(which):
    try:
        if which == 'origin':
            con = mysql.connector.connect(host=dbhost, port=dbport, database=dbname, user=dbuser, password=dbpassword)
        else:
            con = mysql.connector.connect(host=dbhostd, port=dbportd, database=dbnamed, user=dbuserd, password=dbpasswordd)
        if con.is_connected():
            db_Info = con.get_server_info()
            logging.info("Connected to MySQL Server version " + str(db_Info))
            cursor = con.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            logging.info("You're connected to database: " + str(record[0]))
            cursor.close()
    except Error as e:
        logging.error(str(e))
        return 0
    return con



synchronize()