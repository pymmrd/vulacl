#coding=utf-8
from optparse import OptionParser
import tempfile,os,threading,pyinotify,urllib,json,logging
from multiprocessing import Process,Queue
from elasticsearch import Elasticsearch,TransportError
from kafka import KafkaProducer

def main():
    #Set loger
    global logger
    logger=logging.getLogger()
    logger.setLevel(logging.INFO)
    logconsole=logging.StreamHandler()
    logconsole.setLevel(logging.INFO)
    formater=logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    logconsole.setFormatter(formater)
    logger.addHandler(logconsole)
    # Set tcpflow path
    tcpFlowPath = "/usr/bin/tcpflow"
    parser=OptionParser(usage="Usage:python %prog[options]")
    parser.add_option("-a","--args",dest="tcpflow_args",help='tcpflow options,Example:-a "-i eth0 port 80"',default="",type="string")
    parser.add_option("-e","--elasticsearch",dest="es",help="elasticsearch server ip,ip:port",type="string")
    parser.add_option("-i", "--index", dest="index", help="elasticsearch index name", type="string")
    parser.add_option("-t", "--type", dest="type", help="elasticsearch type name", type="string")
    parser.add_option("-k","--kafka",dest="kafka",help="kafka server ip,ip:port",type="string")
    parser.add_option("-T","--topic",dest="topic",help="kafka topic name",type="string")
    parser.add_option("-s", "--screen", dest="screen", help="Out data to screen,True or False", default=False)
    parser.add_option("-l","--log",dest="log",help="log file",type="string")
    (options,args)=parser.parse_args()
    if not options:
        parser.print_help()
    tcpflow_args=options.tcpflow_args
    if options.log:
        logfile=logging.FileHandler(options.log,mode="w")
        logfile.setLevel(logging.INFO)
        logfile.setFormatter(formater)
        logger.addHandler(logfile)
        logger.info("[+]Write log in :%s"%options.log)
    queue = Queue()
    if [bool(options.es),bool(options.kafka),options.screen].count(True)>1:
        logger.critical("[-]Data connot be written to Es/Kafka/Screen at the same time!")
        exit()
    #子进程，处理数据到es\kafka\screen
    elif options.es:
        if not options.index or not options.type:
            logger.critical("[-]Missing index or type name!")
        threadES=Process(target=processES,args=(queue,options.es,options.index,options.type))
        threadES.start()
    elif options.kafka:
        threadKafka=Process(target=processKafka,args=(queue,options.kafka,options.topic))
        threadKafka.start()
    elif options.screen:
        logger.info("[+]Out data to screen!")
        threadScreen=Process(target=processScreen,args=(queue,))
        threadScreen.start()
    elif not options.screen:
        logger.critical("[-]Missing variable:-e or -k or -s")
        exit()
    #子线程，开启并监控TCPFLOW
    tempDir=tempfile.mkdtemp()
    logger.info("[+]TempDir:%s"%tempDir)
    threadPacp=threading.Thread(target=processPcap,args=(tempDir,tcpFlowPath,tcpflow_args))
    threadPacp.start()
    #主进程，监控文件并生成数据
    wm=pyinotify.WatchManager()
    wm.add_watch(tempDir,pyinotify.ALL_EVENTS)
    eventHandler=MonitorFlow(queue)
    notifier=pyinotify.Notifier(wm,eventHandler)
    notifier.loop()
#启动Tcpflow的进程
def processPcap(temDir,tcpFlowPath,tcpflow_args):
    if tcpflow_args:
        tcpflow_args=tcpflow_args.replace('"','')
        logger.info("[+]TcpFlow Command:cd %s && %s -a -e http -Ft %s"%(temDir,tcpFlowPath,tcpflow_args))
    output=os.popen("(cd %s && %s -a -e http -Ft %s)"%(temDir,tcpFlowPath,tcpflow_args))
    logger.info("[+]TcpFlow UP!")
    output.read()
    logger.info("[-]Error:TcpFlow Down!")
#写入数据到ES的进程
def processES(queue,es_host,index,type):
    es=ES(es_host)
    while True:
        record=queue.get()
        es.write_to_es(index,type,record)
#写入数据到kafka的进程
def processKafka(queue,kafka_host,topic):
    kafka=Kafka(kafka_host)
    while True:
        record=json.dumps(queue.get())
        kafka.Send_to_kafka(record,topic)
#输出数据到屏幕的进程
def processScreen(queue):
    while True:
        print queue.get()
#监控流文件，并提取数据
class MonitorFlow(pyinotify.ProcessEvent):
    def __init__(self, queue,pevent=None, **kargs):
        self.queue=queue
        self.pevent = pevent
        self.my_init(**kargs)
    def process_IN_CLOSE_WRITE(self,event):
        data=[]
        try:
            file=open(event.pathname)
            firstLine=file.readline()
            file.close()
            if firstLine[0:4] in ("GET ", "POST"):
                file = open(event.pathname)
                data = self.RequestHandler(file)
                file.close()
            elif firstLine[0:9]=="HTTP/1.1 " and \
                            " Connection " not in firstLine:
                file = open(event.pathname)
                data=self.ResponseHandler(file)
                file.close()
            os.remove(event.pathname)
        except (IOError,OSError):
            pass
        if len(data)>0:
            # Get src_ip src_port dst_ip dst_port From filename
            #add to data
            filename = event.pathname.split("/")[-1]
            [flow_time, ip_port] = filename[0:54].split("T")[0:2]
            flow_time = long(flow_time)
            [src, dst] = ip_port.split("-")
            src = src.split(".")
            dst = dst.split(".")
            src_port = str(int(src[4]))
            dst_port = str(int(dst[4]))
            src_ip = "%s.%s.%s.%s" % (str(int(src[0])), str(int(src[1])), str(int(src[2])), str(int(src[3])))
            dst_ip = "%s.%s.%s.%s" % (str(int(dst[0])), str(int(dst[1])), str(int(dst[2])), str(int(dst[3])))
            for i in range(len(data)):
                data[i]["src_ip"] = src_ip
                data[i]["dst_ip"] = dst_ip
                data[i]["src_port"] = src_port
                data[i]["dst_port"] = dst_port
                data[i]["flow_time"] = flow_time
        for d in data:
            d=self.FillEmpty(d)
            self.queue.put(d)
    #http请求文件处理
    def RequestHandler(self,file):
        data=[]
        post=False
        #Get data From File Content
        for line in file.readlines():
            if line[0:4]=="GET ":
                d={}
                d["http_type"]="Request"
                d["method"]="GET"
                d["uri"]=urllib.quote(line.split()[1])
            elif line[0:5]=="POST ":
                d={}
                d["http_type"] = "Request"
                d["method"]="POST"
                d["uri"] = urllib.quote(line.split()[1])
            elif line[0:6]=="Host: ":
                d["host"]=line[6:-2]
            elif line[0:12]=="User-Agent: ":
                d["user_agent"]=line[11:-2]
            elif line[0:8]=="Cookie: ":
                d["cookie"]=urllib.quote(line[8:-2])
            elif line[0:9]=="Referer: ":
                d["referer"]=line[9:-2]
            elif line[0:16]=="Content-Length: ":
                d["content_length"]=int(line[16:-2])
            elif line[0:14]=="Content-Type: ":
                d["content_type"]=line[14:-2]
            elif line=="\r\n":
                if d["method"]=="GET":
                    data.append(d)
                elif d["method"]=="POST":
                    post=True
            else:
                if post:
                    s=line.split("GET ")
                    if len(s)>1:
                        d["data"]=urllib.quote(s[0])
                        data.append(d)
                        d={}
                        d["method"] = "GET"
                        d["http_type"] = "Request"
                        d["uri"] = urllib.quote(s[-1].split()[0].strip())
                        post=False
                    else:
                        s=line.split("POST ")
                        if len(s) > 1:
                            d["data"] = urllib.quote(s[0])
                            data.append(d)
                            d = {}
                            d["method"] = "POST"
                            d["http_type"] = "Request"
                            d["uri"] = urllib.quote(s[-1].split()[0].strip())
                            post = False
                        else:
                            d["data"]=urllib.quote(line[:-2])
                            data.append(d)
                            post=False
        return data
    def FillEmpty(self,data):
        #对数据没有的字段补空
        fields=['referer', 'http_type', 'host', 'cookie',
               'flow_time', 'src_port', 'uri', 'src_ip',
               'dst_port', 'dst_ip', 'method', 'user_agent',
               "content_type","content_length","status","server",
                "date","data"]
        keys=data.keys()
        empty_field=list(set(fields)^set(keys))
        for e in empty_field:
            data[e]=""
        return data
    #响应数据文件处理
    def ResponseHandler(self,file):
        data=[]
        d={}
        response=False
        while True:
            line=file.readline()
            if line[0:9]=="HTTP/1.1 ":
                if len(d)>0:
                    data.append(d)
                    d={}
                    response=False
                d["http_type"]="Response"
                d["status"]=line.split()[1]
            elif line[0:8]=="Server: ":
                d["server"]=line[8:]
            elif line[0:14]=="Content-Type: ":
                d["content_type"]=line[14:]
            elif line[0:16]=="Content-Length: ":
                d["content_length"]=line[16:]
            elif line[0:6]=="Date: ":
                d["date"]=line[6:]
            elif line=="\r\n":
                if not response:
                    response=True
            elif not line:
                data.append(d)
                break
        return  data
class ES(object):
    def __init__(self,es_host):
        '''init Elastisearch connection'''
        logger.info("[+]Elasticsearch connection success!")
        self.es_connect=Elasticsearch(hosts=es_host)
    def write_to_es(self,index_name,type_name,record):
        '''create a new document '''
        try:
            self.es_connect.index(index_name,type_name,record)
        except TransportError:
            logger.critical("[-]No elasticsearch Index:%s"%index_name)
class Kafka(object):
    def __init__(self,kafka_host):
        self.producer = KafkaProducer(bootstrap_servers=kafka_host)
        logger.info("[+]Kafka connection success!")
    def Send_to_kafka(self, record,topic):
        self.producer.send(topic,record)
if __name__=="__main__":
    main()




