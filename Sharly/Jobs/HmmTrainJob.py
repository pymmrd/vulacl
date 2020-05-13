from __future__ import print_function
from Utils import get_sc,Extractor,Trainer
from pyspark.sql import SQLContext
from hdfs.client import Client
import pickle,time,json,logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(pathname)s/%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
class HmmTrainJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf=conf["App"]["HmmTrainJob"]
    def startJob(self):
        logging.info("[+]Start Job!")
        sc=get_sc(self.app_conf)
        sqlcontext=SQLContext(sc)
        #获取原始数据
        df =sqlcontext.read.json(self.app_conf["data_dir"])
        logging.info("[+]Get data success!")
        rdd=df.toJSON()
        #过滤出请求数据
        p_rdd=rdd.filter(self.filter).cache()
        #抽取参数观察序列
        p_rdd=p_rdd.flatMap(self.extract).cache()
        logging.info("[+]Begin fliter data and extrate parameters......")
        p_list=p_rdd.collect()
        logging.info("[+]Got p_rdd!")
        logging.info("[+]Train data num is:"+str(len(p_list)))
        #按照参数ID分组
        p_dict={}
        for p in p_list:
            if p.keys()[0] not in p_dict.keys():
                p_dict[p.keys()[0]]={}
                p_dict[p.keys()[0]]["p_states"]=[p.values()[0]["p_state"]]
                p_dict[p.keys()[0]]["p_type"]=p.values()[0]["p_type"]
                p_dict[p.keys()[0]]["p_name"] = p.values()[0]["p_name"]
            p_dict[p.keys()[0]]["p_states"].append(p.values()[0]["p_state"])
        logging.info("[+]P num is:"+str(len(p_dict)))
        #检测是否满足最小训练数
        for key in p_dict.keys():
            if len(p_dict[key]["p_states"]) <self.app_conf["min_train_num"]:
                p_dict.pop(key)
        logging.info("[+]Effective p num is:"+str(len(p_dict)))
        models=[]
        logging.info("[+]Begin train models!")
        #参数训练
        trained_num=0
        for p_id in p_dict.keys():
            data={}
            data["p_id"]=p_id
            data["p_states"]=p_dict[p_id]["p_states"]
            trainer=Trainer(data)
            (m,p)=trainer.get_model()
            model = {}
            model["p_id"] = p_id
            model["p_type"]=p_dict[p_id]["p_type"]
            model["p_name"] = p_dict[p_id]["p_name"]
            model["model"] = pickle.dumps(m)
            model["profile"] = p
            models.append(model)
            logging.info("[+]Trained:%s,num is %s"%(p_id,trained_num))
            trained_num+=1
        logging.info("[+]Train Over!")
        #保存训练参数到HDFS
        model_df=sqlcontext.createDataFrame(models)
        logging.info("[+]Trained model num:"+str(model_df.count()))
        date=time.strftime("%Y-%m-%d_%H-%M")
        path="hdfs://%s:8020%smodel%s.json"%(self.app_conf["namenode_model"],self.app_conf["model_dir"],date)
        logging.info("[+]Write model to hdfs,path is :"+path)
        model_df.write.json(path=path)
        logging.info("[+]Job over!")
        sc.stop()
    def filter(self,data):
        #过滤出http请求数据
        data=json.loads(data)
        if data["method"] in ("GET","POST"):
            return True
        else :
            return False
    def extract(self,data):
        flat_data = []
        data=json.loads(data)
        try:
            parameters=Extractor(data).parameter
            for (key,value) in parameters.items():
                flat_data.append({key:value})
        except (UnicodeDecodeError, UnicodeEncodeError):
            logging.info("Error:%s" % str(data))
        return flat_data