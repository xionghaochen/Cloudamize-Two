'''
Created on Jun 9, 2016

@author: walter
'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-

' The second Python project '

__author__ = 'Walter Xiong'

import sys
import getopt
import psycopg2

def main(argv):
    host1=''
    dbname1=''
    port1=''
    user1=''
    password1=''
    host2=''
    dbname2=''
    port2=''
    user2=''
    password2=''
        
    try:
        opts,args=getopt.getopt(argv,"h:d:p:u:w:H:D:P:U:W",["host1=", "dbname1=", "port1=", "user1=", "password1=","host2=", "dbname2=", "port2=", "user2=", "password2="])
    except getopt.GetoptError:
        sys.exit()
        
    for key,value in opts:
        if key in ('-h','--host1'):
            host1=value
        if key in ('-d','--dbname1'):
            dbname1=value
        if key in ('-p','--port1'):
            if value=='':
                port1='5432'
            else:
                port1=value
        if key in ('-u','--user1'):
            if value=='':
                user1='postgres'
            else:
                user1=value
        if key in ('-w','--password1'):
            password1=value
        if key in ('-H','--host2'):
            host2=value
        if key in ('-D','--dbname2'):
            dbname2=value
        if key in ('-P','--port2'):
            if value=='':
                port2='5432'
            else:
                port2=value
        if key in ('-U','--user2'):
            if value=='':
                user2='postgres'
            else:
                user2=value
        if key in ('-W','--password2'):
            password2=value    
    
    connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2)


def connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2):
    # This function is used to connect to specified database. 
    
    conn_string1= "host=%s dbname=%s port=%s user=%s password=%s"%(host1,dbname1,port1,user1,password1)
    print ("Connecting to database\n\n-->%s\n" %(conn_string1))
    
    conn1=psycopg2.connect(conn_string1)
    cursor1=conn1.cursor()
    
    print ("Database %s is connected!\n"%dbname1)
    
    conn_string2= "host=%s dbname=%s port=%s user=%s password=%s"%(host2,dbname2,port2,user2,password2)
    print ("Connecting to database\n\n-->%s\n" %(conn_string2))
    
    conn2=psycopg2.connect(conn_string2)
    cursor2=conn2.cursor()
    
    print ("Database %s is connected!\n"%dbname2)  
    
    cursor1.execute('select pp.proname,pg_get_functiondef(pp.oid) from pg_proc pp inner join pg_namespace pn on (pp.pronamespace = pn.oid) inner join pg_language pl on (pp.prolang = pl.oid) where pl.lanname NOT IN (\'c\',\'internal\') and pn.nspname NOT LIKE \'pg_%\' and pn.nspname <> \'information_schema\' order by pp.proname asc;')
    function1=cursor1.fetchall()
    
    cursor2.execute('select pp.proname,pg_get_functiondef(pp.oid) from pg_proc pp inner join pg_namespace pn on (pp.pronamespace = pn.oid) inner join pg_language pl on (pp.prolang = pl.oid) where pl.lanname NOT IN (\'c\',\'internal\') and pn.nspname NOT LIKE \'pg_%\' and pn.nspname <> \'information_schema\' order by pp.proname asc;')
    function2=cursor2.fetchall()
    
    if function1!=[] and function2!=[]:
        function_compare(dbname1,function1,dbname2,function2)
    else:
        print('There is no function in database %s or %s. Done!'%(dbname1,dbname2))

def function_compare(dbname1,function1,dbname2,function2):
    
    i,x,count=0,0,0
    
    if len(function1)!=len(function2):
        print('The number of functions are not match')
    else:
        while i<len(function1):
            while x<len(function2):
                if function1[i][0]==function2[x][0] and function1[i][1]==function2[x][1]:
                    if (i+1)<len(function1):
                        i=i+1
                        x=0
                    else:
                        break
                elif function1[i][0]==function2[x][0] and function1[i][1]!=function2[x][1]:
                    if (x+1)<len(function2):
                        x=x+1
                    else:
                        count=count+1
                        print('There is no match function %s in database %s\n'%(function1[i][0],dbname2))
                        if (i+1)<len(function1):
                            i=i+1
                            x=0
                        else:
                            break
                elif function1[i][0]!=function2[x][0]:
                    if (x+1)<len(function2):
                        x=x+1
                    else:
                        if (i+1)<len(function1):
                            count=count+1
                            print('There is no match function %s in database %s\n'%(function1[i][0],dbname2))
                            i=i+1
                            x=0
                        else:
                            count=count+1
                            print('There is no match function %s in database %s\n'%(function1[i][0],dbname2))
                            break
            if count==0:
                print('For database %s, the functions might be matching\n'%dbname1)
                break
            else:
                break
        
        while x<len(function2):
            while i<len(function1):
                if function2[x][0]==function1[i][0] and function2[x][1]==function1[i][1]:
                    if (x+1)<len(function2):
                        x=x+1
                        i=0
                    else:
                        break
                elif function2[x][0]==function1[i][0] and function2[x][1]!=function1[i][1]:
                    if (i+1)<len(function1):
                        i=i+1
                    else:
                        count=count+1
                        print('There is no match function %s in database %s\n'%(function2[x][0],dbname1))
                        if (x+1)<len(function2):
                            x=x+1
                            i=0
                        else:
                            break
                elif function2[x][0]!=function1[i][0]:
                    if (i+1)<len(function1):
                        i=i+1
                    else:
                        if (x+1)<len(function2):
                            count=count+1
                            print('There is no match function %s in database %s\n'%(function2[x][0],dbname1))
                            x=x+1
                            i=0
                        else:
                            count=count+1
                            print('There is no match function %s in database %s\n'%(function2[x][0],dbname1))
                            break
            if count==0:
                print('The functions in those two databases are matching\n')
                break
            else:
                print('The functions in those two databases are not matching\n')
                break
    
if __name__=='__main__':
    main(sys.argv[1:])