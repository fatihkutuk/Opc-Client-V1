import mysql.connector

class Mysql:
    def __init__(self, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME):
        self.host = DB_HOST
        self.port = DB_PORT
        self.name = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.MysqlConn = None
    def MysqlConnection(self):
        self.MysqlConn = mysql.connector.connect(host = self.host,
                                    port = self.port,
                                    db = self.name,
                                    user = self.user,
                                    passwd = self.password)
        return self.MysqlConn
    def ReplaceIntoTagOku(self,text):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.callproc('sp_setTagValueOnDataChanged', [text,])
        result = cursor.stored_results()
        cursor.close()
        con.close() 
        return result     
    def GetInit(self):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.callproc('sp_getInit',[])
        result = cursor.stored_results()
        cursor.close()
        con.close() 
        return result         
    def GetDeviceSubsciptionList(self,id):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.callproc('sp_GetDeviceSubscriptionList', [id, ])
        result = cursor.stored_results()
        cursor.close()
        con.close() 
        return result 
    def GetClientSubsciptionList(self,id):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.callproc('sp_getClientSubscriptionList', [id, ])
        result = cursor.stored_results()
        cursor.close()
        con.close() 
        return result 
    def InsertServiceStatus(self,clientId,Status):
        con = self.MysqlConnection()
        cursor = con.cursor() 
        query = "Replace Into Service(ClientId,Status) values (%s,%s)"
        val = (clientId,Status)
        cursor.execute(query,val)
        con.commit()
        cursor.close()
        con.close()
    def GetAllClients(self):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.execute('SELECT cd.clientId FROM channeldevice cd GROUP BY cd.clientId ')
        result = cursor.fetchall()
        cursor.close()
        con.close()
        return result        
    def GetChannelNames(self):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.execute('SELECT cd.channelName, cd.id FROM dbkepware.channeldevice cd')
        result = cursor.fetchall()
        cursor.close()
        con.close()
        return result
    def GetAllDevices(self):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.execute('SELECT id FROM channeldevice ')
        result = cursor.fetchall()
        cursor.close()
        con.close()
        return result          
   
    def GetTagsToWhrite(self,ClientId):
        con = self.MysqlConnection()
        cursor = con.cursor()
        cursor.callproc('sp_getTagsToWrite', [ClientId, ])
        result = cursor.stored_results()
        cursor.close()
        con.close() 
        return result                
