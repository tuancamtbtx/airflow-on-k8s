from elasticsearch import Elasticsearch
from elasticsearch import helpers
from airflow.providers.http.hooks.http import HttpHook
import time


class ElasticSearchHook(HttpHook):
    def __init__(self, elasticsearch_conn_id="", index=""):
        self.elasticsearch_conn_id = elasticsearch_conn_id
        self.index = index
        conn = self.get_connection(elasticsearch_conn_id)
        self.host = conn.host
        self.port = conn.port
        self.user = conn.login
        self.password = conn.password

    def get_client(self):
        es_client = Elasticsearch(
            [self.host],
            scheme="https",
            port=self.port,
            http_auth=(self.user, self.password),
            use_ssl=False
        )
        return es_client

    def parse_data(self, data, es_index):
        actions = []
        for obj in data:
            if "id" in obj:
                action = {
                    "_index": es_index,
                    "_id": obj['id'],
                    "_source": obj,
                }
                actions.append(action)
        return actions

    def build_schema_to_elasticseach_schema(self, bq_schema, is_update):
        TYPE_MAPPING = {
            "INTEGER": "long",
            "INT64": "long",
            "FLOAT": "double",
            "FLOAT64": "double",
            "NUMERIC": "double",
            "STRING": "text"
        }
        mapping_template = {
            "properties": {
            }
        }
        mapping_create_template = {
            "mappings": {
                "properties":{

                }
            }
        }
        fields = bq_schema.get("fields")
        if is_update == True:
            for obj in fields:
                name = obj.get("name")
                dtype = obj.get("type")
                if dtype.upper() in TYPE_MAPPING:
                    props_type = {}
                    props_type["type"] = TYPE_MAPPING[dtype]
                    mapping_template["properties"][name] = props_type
            return mapping_template
        else:
            for obj in fields:
                name = obj.get("name")
                dtype = obj.get("type")
                if dtype.upper() in TYPE_MAPPING:
                    props_type = {}
                    props_type["type"] = TYPE_MAPPING[dtype]
                    mapping_create_template['mappings']["properties"][name] = props_type
            return mapping_create_template

    def bulk(self, data, es_index):
        es_client = self.get_client()
        actions = self.parse_data(data, es_index)
        helpers.bulk(es_client, actions)

    def remove_index(self, es_index):
        es_client = self.get_client()
        self.log.info("remove index: %s", es_index)
        es_client.indices.delete(index=es_index, ignore=[400, 404])

    def alias_index(self,es_index, alias_index):
        es_client = self.get_client()
        if es_client.indices.exists_alias(alias_index) == False:    
            self.log.info("set alias index is not exist: %s", alias_index)
            resp = es_client.indices.put_alias(index=es_index, name=alias_index, ignore=[400,404])
            if 'status' in resp:
                return False
        else:
            self.log.info("set alias index is already exist: %s", alias_index)
            self.remove_index(alias_index)
            resp = es_client.indices.put_alias(index=es_index, name=alias_index, ignore=[400,404])
            if 'status' in resp:
                return False
        return True
    
    def generate_alias_index_by_time(self, es_index):
        ts = time.time()
        return es_index + '_' + str(ts)
    
    def get_key_exist(self,keys, prefix):
        key_arr = []
        for key in keys:
            if key.startswith(prefix):
                key_arr.append(key)
        return key_arr

    def get_alias_by_prefix(self,prefix_index): 
        es_client = self.get_client()
        alias_dict = es_client.indices.get_alias("*")
        keys = alias_dict.keys()
        return self.get_key_exist(keys,prefix_index)

