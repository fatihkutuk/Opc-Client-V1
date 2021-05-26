from logging import Handler
from re import sub
import time
from Classes import Database
from opcua import Client
from opcua import ua
import sys


sys.path.insert(0, "..")
ReplaceIntoTags = [] # Veritabanına yazılacak verilerin tutulacağı dizi
NoErrors = [] #Haberlşeme taglarının tutulacağı dizi
Db = Database.Mysql('localhost',3306,'root','Korusu123','dbkepware') #Veri tabanı sınıdından bir örnek oluşturur
AllClients = Db.GetAllClients() # veritabanındaki clientları getirir
count = 0 # Texte Kaç değer yazdığımızı anlayabilmek için her yazıldığında bu değer 1 artırılır
text = "" #Veritabanına yazma yapılırken bu texte topluca yazılır bu değişken prosedüre gönderilir
NoErrorNodes = [] # Haberlşeme tagları için oluşturulacak nodeların tutulacağı dizi
OpcClientList = []
class SubHandler(object):#Subscribe olunan taglarda değişim olunca tetiklenir

    def datachange_notification(self, node, val, data):
        split_node = str(node).split('.')
        

        if(str(val)!="None"):
            if str(split_node[2])=="_System":
                globals()["text"] =globals()["text"] + "("+str(split_node[1])+",'_NoError',"+str(val)+")"+","
            else:
                globals()["text"] =globals()["text"] + "("+str(split_node[1])+",'"+str(split_node[2])+"',"+str(val)+")"+","

            
        globals()["count"] = globals()["count"] +1
        if globals()["count"] > 5000:
            try:
                Db.ReplaceIntoTagOku(globals()["text"][:-1])
                globals()["count"] = 0
                globals()["text"] = ""
            except:
                True    

handler = SubHandler()
class OpcClient():
    def __init__(self,id):
        self.id = id
        self.client = Client("opc.tcp://127.0.0.1:49320") #default tşme out değeri 4 saniye
        self.nodes = []
        self.sub = None
        self.handle = None
        
    def addNodes(self,nodes):
        self.nodes.append(nodes)


def CreateClients():# veritabanında ki clientlara göre client OpcClient sınıfından örnekler oluşturur
    KepClients = Db.GetAllClients()
    for KepClient in KepClients:
        OpcClientList.append(OpcClient(KepClient[0]))
        
def ConnectClients(): # Client listesindeki clientlara bağlanır
    for OpcClient in OpcClientList:
        OpcClient.client.connect()
        TagList = Db.GetClientSubsciptionList(OpcClient.id)
        for list in TagList:
            list = list.fetchall()
            for tag in list:
                try:
                    OpcClient.nodes.append(OpcClient.client.get_node("ns=2;s="+tag[1]+"."+tag[2]+"."+tag[3]))    
                except:
                    True


def SubscribeClientNodes(): #Diğer taglara subscribe olur
    for OpcClient in OpcClientList:
        OpcClient.sub = OpcClient.client.create_subscription(500, handler)
        try:
            if len(OpcClient.nodes)>0:
                OpcClient.handle = OpcClient.sub.subscribe_data_change(OpcClient.nodes)
            else:
                print("Hiç Node Yok")
        except Exception as e:
            print(e)


def SubscribeNoErrorNodes(): # Haberleşme taglarına subscribe olur
    handler = SubHandler()
    NoErrorClient = Client("opc.tcp://127.0.0.1:49320")
    NoErrorClient.connect()
    sub = NoErrorClient.create_subscription(5000, handler )    
    inits = Db.GetInit()
    for init in inits:
        initfetch = init.fetchall()
        for i in initfetch:
            NoErrorNodes.append(NoErrorClient.get_node("ns=2;s="+i[0]+"."+str(i[2])+"._System._NoError"))     
    try:
        if len(NoErrorNodes)>0:
            sub.subscribe_data_change(NoErrorNodes)
        else:
            print("Hiç Node Yok")    
    except Exception as e:
        True

# def SubscribeClientNodes():# Diğer tüm taglarına subscribe olur

#     KepClients = Db.GetAllClients()
#     client = Client("opc.tcp://127.0.0.1:49320")
#     client.connect()
#     for kep_client in KepClients:

#         kep_client_sub = client.create_subscription(500,handler)

#         Nodes = []
#         KepClientTags = Db.GetClientSubsciptionList(kep_client[0])

#         for tags in KepClientTags:  
#             tags = tags.fetchall()
#             for tag in tags:
#                 try:
#                     Nodes.append(client.get_node("ns=2;s="+tag[1]+"."+tag[2]+"."+tag[3]))
#                 except:
#                     True
#             try:             
#               kep_client_sub.subscribe_data_change(Nodes)

#             except:
#                 True
#         # kep_client_sub.delete()
#         # client.disconnect()    
TagYazClient = Client("opc.tcp://127.0.0.1:49320")
def TagYaz():#Tagyaz tablosundaki tagları yazar
    TagYazClient.connect()
    for clients in AllClients:
        SetNodes = []
        SetValues = []
        try:
            TagsToWhrite = Db.GetTagsToWhrite(clients[0])
        except:
            False    
        for tag in TagsToWhrite:
            tag = tag.fetchall()
            for t in tag:
                try:
                    node = TagYazClient.get_node("ns=2;s="+t[1]+"."+t[0]+"."+t[2])
                    SetNodes.append(node)
                    varianttype = node.get_data_type_as_variant_type()
                    if varianttype==ua.VariantType.Int16:
                        dv = ua.DataValue(ua.Variant(int(t[3]), varianttype))
                    else:    
                        dv = ua.DataValue(ua.Variant(t[3], varianttype))
                    SetValues.append(dv)
                except Exception as e:
                    True
        try:
            if len(SetValues)>0:
                TagYazClient.set_values(SetNodes,SetValues)
                SetNodes.clear()
                SetValues.clear()   
        except:
            False    
    time.sleep(0.5)
    TagYazClient.disconnect()
    


 
if __name__ == '__main__':
    SubscribeNoErrorNodes()
    CreateClients()
    ConnectClients()
    SubscribeClientNodes()
    while 1:
       TagYaz() 
    #SubscribeNoErrorNodes()
    #SubscribeSystemNodes()
    #TagYaz()

