from __future__ import print_function
from Utils import get_sc,Extractor,ES
from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
import numpy as np
import pickle,warnings,logging,json
warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(pathname)s/%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
class HmmDetectionJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf=conf["App"]["HmmDetectionJob"]
    def startJob(self):
        logging.info("[+]Start Job!")
        sc = get_sc(self.app_conf)
        sqlcontext = SQLContext(sc)
        #获取模型
        model_data = sqlcontext.read.json(self.app_conf["model_dir"]).collect()
        model_keys=[0]*len(model_data)
        for index,model_d in enumerate(model_data):
            model_keys[index]=model_d["p_id"]
        logging.info("[+]Got model success!")
        logging.info("[+]Start Streaming!")
        ssc=StreamingContext(sc,20)
        model_data = ssc._sc.broadcast(model_data)
        model_keys = ssc._sc.broadcast(model_keys)
        logging.info("[+]Broadcast model and keys success!")
        zookeeper = self.app_conf["zookeeper"]
        in_topic = self.app_conf["in_topic"]
        in_topic_partitions = self.app_conf["in_topic_partitions"]
        topic = {in_topic: in_topic_partitions}
        #获取kafka数据
        dtream = KafkaUtils.createStream(ssc, zookeeper, self.app_conf["app_name"], topic)
        dtream=dtream.filter(self.filter)
        dtream.foreachRDD(
           lambda rdd: rdd.foreachPartition(
               lambda iter:self.detector(iter,model_data,model_keys)
           )
        )
        ssc.start()
        ssc.awaitTermination()
    def filter(self,data):
        #过滤出http请求数据
        data=json.loads(data[1])
        if data["method"] in ("GET","POST"):
            return True
        else :
            return False
    def detector(self, iter,model_data,model_keys):
        es = ES(self.app_conf["elasticsearch"])
        index_name = self.app_conf["index_name"]
        type_name = self.app_conf["type_name"]
        model_data=model_data.value
        model_keys=model_keys.value
        for record in iter:
            record=json.loads(record[1])
            try:
                parameters = Extractor(record).parameter
                for (p_id, p_data) in parameters.items():
                    if p_id in model_keys:
                        model_d = model_data[model_keys.index(p_id)]
                        model = pickle.loads(model_d.model)
                        profile = model_d.profile
                        score = model.score(np.array(p_data["p_state"]))
                        if score < profile:
                            alarm = ES.pop_null(record)
                            alarm["alarm_type"] = "HmmParameterAnomaly"
                            alarm["p_id"] = p_id
                            alarm["p_name"] = model_d.p_name
                            alarm["p_type"] = model_d.p_type
                            alarm["p_profile"] = profile
                            alarm["score"] = score
                            es.write_to_es(index_name, type_name, alarm)
            except (UnicodeDecodeError, UnicodeEncodeError):
                logging.info("Error:%s" % str(record))