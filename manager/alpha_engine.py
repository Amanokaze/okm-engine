import os
import time
import json
import threading
import csv
import atexit
import stmt
from db import DB
from psycopg2 import DatabaseError
from datetime import date, datetime, timedelta
from kubernetes import client

DEFAULT_INFO = {
    "KUBE_HOST": "http://localhost:6443",
    "KUBE_API_KEY": None,
    "KUBE_MGR_ST_INTERVAL": 10,
    "KUBE_MGR_LT_INTERVAL": 600,
    "KUBE_CLUSTER_NAME": "kubernetes",
}

KUBE_HOST = os.environ["KUBE_HOST"] if "KUBE_HOST" in os.environ else DEFAULT_INFO["KUBE_HOST"]
KUBE_API_KEY = os.environ["KUBE_API_KEY"] if "KUBE_API_KEY" in os.environ else DEFAULT_INFO["KUBE_API_KEY"]
KUBE_MGR_ST_INTERVAL = int(os.environ["KUBE_MGR_ST_INTERVAL"])  if "KUBE_MGR_ST_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_ST_INTERVAL"]
KUBE_MGR_LT_INTERVAL = int(os.environ["KUBE_MGR_LT_INTERVAL"])  if "KUBE_MGR_LT_INTERVAL" in os.environ else DEFAULT_INFO["KUBE_MGR_LT_INTERVAL"]
KUBE_CLUSTER_NAME = os.environ["KUBE_CLUSTER_NAME"] if "KUBE_CLUSTER_NAME" in os.environ else DEFAULT_INFO["KUBE_CLUSTER_NAME"]
#KUBE_MGR_ST_INTERVAL = DEFAULT_INFO["KUBE_MGR_ST_INTERVAL"]

LOGFILE_NAME = "manager"
ONE_DAY_SECONDS = 86400

class Log:
    def logfile_check(self):
        self._file_name = f"{LOGFILE_NAME}_{datetime.now().strftime('%y%m%d')}.log"
        
        if not os.path.isfile(self._file_name):
            file = open(self._file_name, "wt", encoding="utf8")
            file.close()

    def __init__(self):
        self.logfile_check()

    def write(self, log_type, message=None):
        self.logfile_check()
        statement = f"[{log_type}] {datetime.now().strftime('%m/%d %H:%M:%S')} - {message}"

        file = open(self._file_name, "a", encoding="utf8")
        file.write(statement)
        file.write("\n")
        file.close()

        if log_type == "Error":
            print(message)

class SYSTEM:
    def __init__(self):
        self._duration = 0
        self._network_metric = {
            "lastrealtimeperf": dict(),
            "nodeperf": dict(),
            "podperf": dict(),
            "podnet": dict()
        }

    def refresh_duration(self):
        self._duration = self._duration + KUBE_MGR_ST_INTERVAL

    def get_duration(self):
        return self._duration

    def set_network_metric(self, net_type, item, data):
        self._network_metric[net_type][item] = data

    def get_network_metric(self, net_type, item):
        return self._network_metric[net_type][item] if item in self._network_metric[net_type] else None

class Engine:
    def __init__(self):
        # Kube Cluster 접속은 최초 1회만 이루어지며, Thread 별로 접속을 보장하지 않음
        self.kube_cfg = client.Configuration()
        self.kube_cfg.api_key['authorization'] = KUBE_API_KEY
        self.kube_cfg.api_key_prefix['authorization'] = 'Bearer'
        self.kube_cfg.host = KUBE_HOST
        self.kube_cfg.verify_ssl = True
        self.kube_cfg.ssl_ca_cert = 'ca.crt'

        self.log = Log()
        self.db = DB()
        self.system_var = SYSTEM()

        self.start()

    def start(self):
        # onTune DB Connection을 위한 Connection Pool 역시 사전에 생성토록 함
        # Connection Pool이 정상 생성될 경우에만 최초 Thread를 가동토록 할 것
        try:
            self.db.create_connection()
            atexit.register(self.db.shutdown_connection_pool)
            threading.Thread(target=self.thread_func).start()

        except DatabaseError as e:
            self.log.write("ERROR", str(e))

    def thread_func(self):
        # 기본 구조는 KUBE_MGR_ST_INTERVAL 간격으로 Thread를 생성해서 기능을 수행하도록 함
        threading.Timer(KUBE_MGR_ST_INTERVAL, self.thread_func).start()
        self.system_var.refresh_duration()
        
        # API 기본 정보를 가져오는 부분
        # Cluster name 정보는 API를 통해서 가져올 수 없는 것으로 최종 확인되었음
        api_version_info = client.CoreApi(client.ApiClient(self.kube_cfg)).get_api_versions()
        kube_basic_info = {
            "manager_name": self.db.get_basic_info("managername"),
            "manager_ip": self.db.get_basic_info("host"),
            "cluster_name": KUBE_CLUSTER_NAME,
            "cluster_address": api_version_info.server_address_by_client_cid_rs[0].server_address.split(":")[0]
        }
        
        # Stats API를 사용해서 데이터를 가져오는 부분
        api = client.CoreV1Api(client.ApiClient(self.kube_cfg))
        (ndata, nmdata, pmdata) = self.get_kube_node_data(api)
        kube_data = {
            "node": ndata,
            "node_metric": nmdata,
            "pod_metric": pmdata,
            "ns": self.get_kube_ns_data(api)
        }

        # 데이터 가져오는 부분이 실패하면 onTune DB의 입력 의미가 없어지므로 Thread를 종료함
        if not kube_data["node"] or not kube_data["ns"]:
            return

        # onTune DB Schema Check 및 데이터를 입력하는 부분
        schema_obj = self.check_ontune_schema()
        result = self.put_kube_data_to_ontune(kube_basic_info, kube_data, schema_obj)

        if result:
            pass

    def change_quantity_unit(self, value):
        try:
            return int(value)
        except ValueError:
            if "Ki" in value:
                return int(value[:-2]) * 1024
            elif "Mi" in value:
                return int(value[:-2]) * 1048576
            elif "Gi" in value:
                return int(value[:-2]) * 1073741824
            elif "m" in value:
                return int(value[:-1]) * 0.001
            elif "e" in value:
                return int(value[:-2]) * (10 ** int(value[-1]))
            elif "k" in value:
                return int(value[:-1]) * 1000
            elif "M" in value:
                return int(value[:-1]) * 1000000
            elif "G" in value:
                return int(value[:-1]) * 1000000000
            else:
                return 0

    def input_tableinfo(self, name, cursor, conn, ontunetime=0):
        ontunetime = self.get_ontunetime(cursor) if ontunetime == 0 else ontunetime
        cursor.execute(stmt.SELECT_TABLEINFO_TABLENAME.format(name))
        result = cursor.fetchone()

        exec_stmt = stmt.UPDATE_TABLEINFO.format(ontunetime, name) if result[0] == 1 else stmt.INSERT_TABLEINFO.format(ontunetime, name)
        cursor.execute(exec_stmt)
        conn.commit()
        self.log.write("PUT", f"{name} data is added in kubetableinfo table.")

    def insert_columns_ref(self, schema, obj_name):
        return ",".join(list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x, schema["reference"][obj_name]))))

    def insert_columns_metric(self, schema, obj_name):
        metric_list = list(schema["reference"][obj_name]) if obj_name == "kubelastrealtimeperf" else list(schema["metric"][obj_name])
        return ",".join(list(y[0] for y in metric_list))

    def insert_values(self, data):
        return ",".join(list(f"'{x}'" for x in data))

    def select_average_columns(self, schema, obj_name, key_columns):
        not_averaged_columns = key_columns + ['_ontunetime','_agenttime']
        metric_list = list(x[0] for x in schema["metric"][obj_name] if x[0] not in not_averaged_columns)
        return ",".join(key_columns + list(f"round(avg({x}))::int8 {x}" for x in metric_list))

    def update_values(self, schema, obj_name, data):
        not_included_column_list = ("_managerid","_clusterid")
        column_list = list(y[0] for y in list(filter(lambda x: 'PRIMARY KEY' not in x and x[0] not in not_included_column_list, schema["reference"][obj_name])))
        value_list = list(data.values())
        stmt_list = list(f"{column_list[i]}='{value_list[i]}'" for i in range(len(column_list)))

        return ",".join(stmt_list)

    def get_ontunetime(self, cursor):
        cursor.execute(stmt.SELECT_ONTUNEINFO)
        result = cursor.fetchone()
        return result[0] if result else 0

    def get_agenttime(self, cursor):
        cursor.execute(stmt.SELECT_ONTUNEINFO_BIAS)
        result = cursor.fetchone()
        return result[0]+(result[1]*60) if result else 0

    def svtime_to_timestampz(self, svtime):
        tz_interval = ONE_DAY_SECONDS - int(time.mktime(time.gmtime(ONE_DAY_SECONDS)))
        return int(time.mktime((datetime.strptime(svtime, '%Y-%m-%dT%H:%M:%SZ') + timedelta(seconds=tz_interval)).timetuple()))

    def calculate(self, cal_type, values):
        NANO_VALUES = 10 ** 9
        KILO_VALUES = 1024
        MEGA_VALUES = KILO_VALUES ** 2
        PERCENT_VALUE = 100
        DUPLICATE_VALUE = 100
        
        if cal_type == "cpu_usage_percent":
            # Reference: https://github.com/kubernetes/heapster/issues/650#issuecomment-147795824
            vals = list(int(x) if x else 0 for x in values)
            return round(vals[0] / (vals[1] * NANO_VALUES) * PERCENT_VALUE)
        elif cal_type[:6] == "memory":
            wsbytes = int(values["workingSetBytes"]) if "workingSetBytes" in values else 0
            avbbytes = int(values["availableBytes"]) if "availableBytes" in values else 0
            rssbytes = int(values["rssBytes"]) if "rssBytes" in values else 0
            limitbytes = wsbytes + avbbytes

            if cal_type == "memory_used_percent":
                # Reference: WSBytes / (WSBytes + Available = Limit) (Now not used UsageBytes)
                # WSBytes: The amount of working set memory. This includes recently accessed memory, dirty memory, and kernel memory. WorkingSetBytes is <= UsageBytes
                # UsageBytes: Total memory in use. This includes all memory regardless of when it was accessed.        
                return round(wsbytes / limitbytes * PERCENT_VALUE * DUPLICATE_VALUE) if limitbytes > 0 else 0
            elif cal_type == "memory_swap_percent":
                # Reference: RSSBytes / (WSBytes + Available = Limit)
                # RSSBytes: The amount of anonymous and swap cache memory (includes transparent hugepages)
                return round(rssbytes / limitbytes * PERCENT_VALUE * DUPLICATE_VALUE) if limitbytes > 0 else 0
            elif cal_type == "memory_size":
                return limitbytes
            else:
                return 0
        elif cal_type == "network":
            return round(int(values[0]) / KILO_VALUES)
        elif cal_type[:2] == "fs":
            totalbytes = int(values["capacityBytes"]) if "capacityBytes" in values else 0
            freebytes = int(values["availableBytes"]) if "availableBytes" in values else 0
            usedbytes = int(values["usedBytes"]) if "usedBytes" in values else 0

            itotalbytes = int(values["inodes"]) if "inodes" in values else 0
            ifreebytes = int(values["inodesFree"]) if "inodesFree" in values else 0
            iusedbytes = int(values["inodesUsed"]) if "inodesUsed" in values else 0

            if cal_type == "fs_usage_percent":
                # Reference: UsedBytes / TotalBytes
                # UsedBytes - This may differ from the total bytes used on the filesystem and may not equal CapacityBytes - AvailableBytes.
                return round(usedbytes / totalbytes * PERCENT_VALUE) if totalbytes > 0 else 0
            elif cal_type == "fs_total_size":
                return round(totalbytes / MEGA_VALUES)
            elif cal_type == "fs_free_size":
                return round(freebytes / MEGA_VALUES)        
            elif cal_type == "fs_inode_usage_percent":
                # Reference: inodesUsed / inodes
                return round(iusedbytes / itotalbytes * PERCENT_VALUE) if itotalbytes > 0 else 0
            elif cal_type == "fs_inode_total_size":
                return round(itotalbytes / KILO_VALUES)
            elif cal_type == "fs_inode_free_size":
                return round(ifreebytes / KILO_VALUES)
            else:
                return 0
        else:
            return 0

    def get_kube_node_data(self, api):
        try:
            nodes = api.list_node()
            node_data = dict()
            node_metric_data = dict()
            pod_metric_data = dict()
            
            for node in nodes.items:
                nodename = node.metadata.name

                node_data[nodename] = {
                    "uid": node.metadata.uid,
                    "name": nodename,
                    "nameext": nodename,
                    "enabled": 1,
                    "state": 1,
                    "connected": 1,
                    "osimage": node.status.node_info.os_image,
                    "osname": node.status.node_info.operating_system,
                    "containerruntimever": node.status.node_info.container_runtime_version,
                    "kubeletver": node.status.node_info.kubelet_version,
                    "kubeproxyver": node.status.node_info.kube_proxy_version,
                    "cpuarch": node.status.node_info.architecture,
                    "cpucount": node.status.capacity["cpu"],
                    "ephemeralstorage": self.change_quantity_unit(node.status.capacity["ephemeral-storage"]),
                    "memorysize": self.change_quantity_unit(node.status.capacity["memory"]),
                    "pods": node.status.capacity["pods"],
                    "ip": node.status.addresses[0].address
                }

                try:
                    node_stats = api.connect_get_node_proxy_with_path(nodename, "stats/summary")
                    node_stats_json = json.loads(node_stats.replace("'",'"'))
                    node_metric_data[nodename] = node_stats_json['node']
                    pod_metric_data[nodename] = node_stats_json['pods']
                except client.rest.ApiException as e:
                    node_metric_data[nodename] = dict()
                    pod_metric_data[nodename] = dict()
                    node_data[nodename]["state"] = 0
                    node_data[nodename]["connected"] = 0

            self.log.write("GET", "Kube Node Data Import is completed.")

            return (node_data, node_metric_data, pod_metric_data)

        except Exception as e:
            self.log.write("Error", str(e))
            return (False, False, False)

    def get_kube_ns_data(self, api):
        try:
            nslist = api.list_namespace()
            return list(x.metadata.name for x in nslist.items)
        except Exception as e:
            self.log.write("Error", str(e))
            return False

    def check_ontune_schema(self):
        # Load onTune Schema
        schema_object = {
            "reference": dict(),
            "metric": dict(),
            "index": dict()
        }
        with open('schema.csv', 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            for row in reader:
                (schema_type, obj_name) = row[:2]

                if obj_name not in schema_object[schema_type]:
                    schema_object[schema_type][obj_name] = list()

                properties = list(filter(lambda x: x != "", row[2:]))
                schema_object[schema_type][obj_name].append(properties)

        with self.db.get_resource_rdb() as (cursor, _, conn):
            # Check Reference Tables
            for obj_name in schema_object["reference"]:
                properties = schema_object["reference"][obj_name]
                cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_REF.format(obj_name))
                result = cursor.fetchone()

                if result[0] == 1:
                    self.log.write("GET", f"Reference table {obj_name} is checked.")
                else:
                    self.log.write("GET", f"Reference table {obj_name} doesn't exist. now it will be created.")
                    creation_prefix = "create table if not exists"
                    column_properties = ",".join(list(" ".join(x) for x in properties))
                    table_creation_statement = f"{creation_prefix} {obj_name} ({column_properties});"
                    cursor.execute(table_creation_statement)
                    conn.commit()
                    self.log.write("PUT", f"Reference table {obj_name} creation is completed.")

                    self.input_tableinfo(obj_name, cursor, conn)
            
            # Check Metric Tables
            for obj_name in schema_object["metric"]:
                properties = schema_object["metric"][obj_name]
                table_postfix = f"_{datetime.now().strftime('%y%m%d')}00"
                cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_MET.format(obj_name, table_postfix))
                result = cursor.fetchone()

                if result[0] == 2:
                    self.log.write("GET", f"Realtime/avg{obj_name}{table_postfix} metric tables are checked.")
                else:
                    # Metric Table Creation
                    self.log.write("GET", f"Realtime/avg{obj_name}{table_postfix} metric tables doesn't exist. now they will be created.")
                    creation_prefix = "create table if not exists"
                    column_properties = ",".join(list(" ".join(x) for x in properties))

                    for table_prefix in ('realtime','avg'):
                        full_table_name = f"{table_prefix}{obj_name}{table_postfix}"
                        table_creation_statement = f"{creation_prefix} {full_table_name} ({column_properties});"
                        cursor.execute(table_creation_statement)
                        conn.commit()
                        self.log.write("PUT", f"Metric table {full_table_name} creation is completed.")

                        # Metric Table Index Creation
                        index_properties = ",".join(schema_object["index"][obj_name][0])
                        index_creation_statement = f"create index if not exists i{full_table_name} on public.{full_table_name} using btree ({index_properties});"
                        cursor.execute(index_creation_statement)
                        conn.commit()
                        self.log.write("PUT", f"Metric table index i{full_table_name} creation is completed.")

                        self.input_tableinfo(full_table_name, cursor, conn)

        return schema_object

    def put_kube_data_to_ontune(self, info, kube_data, schema_obj):
        # Put data
        with self.db.get_resource_rdb() as (cursor, cur_dict, conn):
            # Kube Data variables
            namespace_list = kube_data["ns"]     
            node_list = kube_data["node"]

            # Pre-define data (여기에서의 Key는 Primary와 같은 Key값이 아니라 dictionary의 key-value의 key를 뜻함)
            manager_id = 0
            cluster_id = 0
            namespace_query_dict = dict()
            node_query_dict = dict()            # Key: Nodename
            node_sysco_query_dict = dict()      # Key: Nodeid
            pod_query_dict = dict()             # Key: UID
            pod_container_query_dict = dict()   # Key: Podid
            pod_device_query_dict = dict()      # Key: Devicetype


            # Check Managerinfo table
            try:
                cursor.execute(stmt.SELECT_MANAGERINFO_IP.format(info["manager_ip"]))
                result = cursor.fetchone()
                manager_id = result[0]
            except:            
                column_data = self.insert_columns_ref(schema_obj, "kubemanagerinfo")
                value_data = self.insert_values([info["manager_name"], info["manager_name"], info["manager_ip"]])
                cursor.execute(stmt.INSERT_TABLE.format("kubemanagerinfo", column_data, value_data))
                conn.commit()
                self.input_tableinfo("kubemanagerinfo", cursor, conn)
                #self.log.write("PUT", f"Kubemanagerinfo insertion is completed - {info['manager_ip']}")

                cursor.execute(stmt.SELECT_MANAGERINFO_IP.format(info["manager_ip"]))
                result = cursor.fetchone()
                manager_id = result[0]

            if not manager_id:
                #self.log.write("GET", "Kubemanagerinfo has an error. Put data process is stopped.")
                return False

            # Check Clusterinfo table
            try:
                cursor.execute(stmt.SELECT_CLUSTERINFO_IP_MGRID.format(info["cluster_address"], manager_id))
                result = cursor.fetchone()
                cluster_id = result[0]
            except:
                column_data = self.insert_columns_ref(schema_obj, "kubeclusterinfo")
                value_data = self.insert_values([manager_id, info["cluster_name"], info["cluster_name"], info["cluster_address"]])
                cursor.execute(stmt.INSERT_TABLE.format("kubeclusterinfo", column_data, value_data))
                conn.commit()
                self.input_tableinfo("kubeclusterinfo", cursor, conn)
                #self.log.write("PUT", f"Kubeclusterinfo insertion is completed - {info['cluster_address']}")

                cursor.execute(stmt.SELECT_CLUSTERINFO_IP_MGRID.format(info["cluster_address"], manager_id))
                result = cursor.fetchone()
                cluster_id = result[0]

            if not cluster_id:
                #self.log.write("GET", "Kubeclusterinfo has an error. Put data process is stopped.")
                return False

            # Check Namespace(NS) table
            try:       
                cur_dict.execute(stmt.SELECT_NAMESPACEINFO_CLUSTERID.format(cluster_id))
                namespace_query_dict = dict({x["_nsname"]:x for x in cur_dict.fetchall()})
            except:
                pass
                
            try:
                new_namespace_list = list(filter(lambda x: x not in namespace_query_dict.keys(), namespace_list))
                old_namespace_list = dict(filter(lambda x: x[0] not in namespace_list, namespace_query_dict.items()))
                old_ns_id_list = list(str(x[1]["_nsid"]) for x in old_namespace_list.items())
                
                # New Namespace Insertion
                for new_ns in new_namespace_list:
                    column_data = self.insert_columns_ref(schema_obj, "kubensinfo")
                    value_data = self.insert_values([cluster_id, new_ns, 1])
                    cursor.execute(stmt.INSERT_TABLE.format("kubensinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubensinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubensinfo insertion is completed - {new_ns}")

                # Old Namespace Update
                if len(old_ns_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubensinfo", "_nsid", ",".join(old_ns_id_list)))
                    conn.commit()
                    self.input_tableinfo("kubensinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubensinfo enabled state is updated - {','.join(old_ns_id_list)}")
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenamespaceinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Nodeinfo Table
            try:
                cur_dict.execute(stmt.SELECT_NODEINFO_CLUSTERID.format(cluster_id))
                node_query_dict = dict({x["_nodename"]:x for x in cur_dict.fetchall()})
            except:
                pass

            # Namespace, Nodesysco, Pod 등의 정보는 과거 정보는 enabled=0으로 갱신, 신규 정보는 insert하도록하나,
            # Node 정보는 추가로 기존 정보의 변경사항에 대해서 Update하는 부분이 추가되므로 프로세스도 달라짐
            try:
                for node in node_list:
                    if node in node_query_dict:
                        if node_list[node]["uid"] != node_query_dict[node]["_nodeuid"]:
                            update_data = self.update_values(schema_obj, "kubenodeinfo", node_list[node])
                            cursor.execute(stmt.UPDATE_NODEINFO.format(update_data, node_query_dict[node]["_nodeid"]))
                            conn.commit()
                            self.input_tableinfo("kubenodeinfo", cursor, conn)
                            #self.log.write("PUT", f"Kubenodeinfo information is updated - {node}")

                    else:
                        column_data = self.insert_columns_ref(schema_obj, "kubenodeinfo")
                        value_data = self.insert_values([manager_id, cluster_id] + list(node_list[node].values()))
                        cursor.execute(stmt.INSERT_TABLE.format("kubenodeinfo", column_data, value_data))
                        conn.commit()
                        self.input_tableinfo("kubenodeinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubenodeinfo insertion is completed - {node}")

                old_node_list = dict(filter(lambda x: x[0] not in node_list, node_query_dict.items()))
                old_node_id_list = list(str(x[1]["_nodeid"]) for x in old_node_list.items())

                # Old Node Update
                if len(old_node_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubenodeinfo", "_nodeid", ",".join(old_node_id_list)))
                    conn.commit()
                    self.input_tableinfo("kubenodeinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubenodeinfo enabled state is updated - {','.join(old_node_id_list)}")

                # New Node ID Update
                try:
                    cur_dict.execute(stmt.SELECT_NODEINFO_CLUSTERID.format(cluster_id))
                    result = cur_dict.fetchall()
                    node_query_dict = dict({x["_nodename"]:x for x in result})
                except:
                    pass
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenodeinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            if not node_query_dict:
                self.log.write("GET", "Kubenodeinfo is empty. Put data process is stopped.")
                return True         # Not False return

            # Check Node Systemcontainer info Table
            try:
                syscontainer_info_dict = dict({x[0]:list(y["name"] for y in x[1]["systemContainers"]) for x in kube_data["node_metric"].items()})
                syscontainer_info = list()
                sc_query_list = list()

                for node in syscontainer_info_dict:
                    syscontainer_info.extend(list({"nodename":node, "containername": x} for x in syscontainer_info_dict[node]))

                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_NODE_SYSCONTAINER_NODEID.format(nodeid_data))
                    sc_query_list = list(dict(x) for x in cur_dict.fetchall())
                except:
                    pass

                new_node_sysco_list = list(filter(lambda x: [x["nodename"], x["containername"]] not in list([y["_nodename"],y["_containername"]] for y in sc_query_list), syscontainer_info))
                old_node_sysco_list = list(filter(lambda x: [x["_nodename"], x["_containername"]] not in list([y["nodename"],y["containername"]] for y in syscontainer_info), sc_query_list))

                for sysco in new_node_sysco_list:
                    nodeid = node_query_dict[sysco["nodename"]]["_nodeid"]
                    column_data = self.insert_columns_ref(schema_obj, "kubenodesyscoinfo")
                    value_data = self.insert_values([nodeid, sysco["containername"], 1])
                    cursor.execute(stmt.INSERT_TABLE.format("kubenodesyscoinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubenodesyscoinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubenodesyscoinfo insertion is completed - {sysco['nodename']} / {sysco['containername']}")

                if old_node_sysco_list:
                    old_node_sysco_id_list = list(str(x["_syscontainerid"]) for x in old_node_sysco_list)

                    if len(old_node_sysco_id_list) > 0:
                        cursor.execute(stmt.UPDATE_ENABLED.format("kubenodesyscoinfo","_syscontainerid",",".join(old_node_sysco_id_list)))
                        conn.commit()
                        self.input_tableinfo("kubenodesyscoinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubenodesyscoinfo enabled state is updated - {','.join(old_node_id_list)}")

                # New Node System Container info Update
                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_NODE_SYSCONTAINER_NODEID.format(nodeid_data))
                    result = cur_dict.fetchall()

                    for row in result:
                        nodeid = row["_nodeid"]
                        if nodeid not in node_sysco_query_dict:
                            node_sysco_query_dict[nodeid] = list()

                        node_sysco_query_dict[nodeid].append(row)
                except:
                    pass

            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubenodesystemcontainer info has an error. Put data process is stopped. - {str(e)}")
                return False
                
            # Check Podinfo table
            try:
                podinfo_dict = dict({x[0]:list(y["podRef"] for y in x[1]) for x in kube_data["pod_metric"].items()})
                pod_info = list()
                for node in podinfo_dict:
                    pod_info.extend(list({
                        "name": x["name"],
                        "namespace": x["namespace"],
                        "uid": x["uid"],
                        "node": node
                    } for x in podinfo_dict[node]))

                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_PODINFO_NODEID.format(nodeid_data))
                    pod_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
                except:
                    pass

                new_pod_list = list(filter(lambda x: x["uid"] not in pod_query_dict.keys(), pod_info))
                old_pod_list = dict(filter(lambda x: x[0] not in list(y["uid"] for y in pod_info), pod_query_dict.items()))
                old_pod_id_list = list(str(x[1]["_podid"]) for x in old_pod_list.items())

                for pod in new_pod_list:
                    nodeid = node_query_dict[pod["node"]]["_nodeid"]
                    nsid = namespace_query_dict[pod["namespace"]]["_nsid"]
                    column_data = self.insert_columns_ref(schema_obj, "kubepodinfo")
                    value_data = self.insert_values([nodeid, nsid, pod["uid"], pod["name"], 1])
                    cursor.execute(stmt.INSERT_TABLE.format("kubepodinfo", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubepodinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubepodinfo insertion is completed - {pod['node']}/{pod['name']}")

                # Old Pod Update
                if len(old_pod_id_list) > 0:
                    cursor.execute(stmt.UPDATE_ENABLED.format("kubepodinfo", "_podid", ",".join(old_pod_id_list)))
                    conn.commit()
                    self.input_tableinfo("kubepodinfo", cursor, conn)
                    #self.log.write("PUT", f"Kubepodinfo enabled state is updated - {','.join(old_pod_id_list)}")

                # New Pod info Update
                try:
                    nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                    cur_dict.execute(stmt.SELECT_PODINFO_NODEID.format(nodeid_data))
                    pod_query_dict = dict({x["_uid"]:x for x in cur_dict.fetchall()})
                except:
                    pass

            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubepodinfo has an error. Put data process is stopped. - {str(e)}")
                return False

            # Check Container and Pod deviceinfo table                        
            if podinfo_dict:
                # Check Container table
                try:
                    container_info = list()
                    container_query_list = list()
                    
                    containerinfo_dict = dict({x[0]:dict({
                        y["podRef"]["uid"]:y["containers"] for y in x[1]
                    }) for x in kube_data["pod_metric"].items()})

                    for node in containerinfo_dict:
                        for pod in containerinfo_dict[node]:
                            container_info.extend(list({
                                "name": x["name"],
                                "starttime": x["startTime"],
                                "pod": pod,
                                "node": node
                            } for x in containerinfo_dict[node][pod]))

                    try:
                        nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                        cur_dict.execute(stmt.SELECT_CONTAINERINFO_NODEID.format(nodeid_data))
                        container_query_list = list(dict(x) for x in cur_dict.fetchall())
                    except:
                        pass

                    # Container 정보는 추가만 되며, 삭제나 enabled를 별도로 설정하지 않음
                    new_container_list = list(filter(lambda x: [x["node"],x["pod"],x["name"]] not in list([y["_nodename"],y["_poduid"],y["_containername"]] for y in container_query_list), container_info))

                    for container in new_container_list:
                        podid = pod_query_dict[container["pod"]]["_podid"]
                        column_data = self.insert_columns_ref(schema_obj, "kubecontainerinfo")
                        value_data = self.insert_values([podid, container["name"], self.svtime_to_timestampz(container["starttime"])])
                        cursor.execute(stmt.INSERT_TABLE.format("kubecontainerinfo", column_data, value_data))
                        conn.commit()
                        self.input_tableinfo("kubecontainerinfo", cursor, conn)
                        #self.log.write("PUT", f"Kubecontainerinfo insertion is completed - {podid} / {container['name']}")

                    # New Pod Container info Update
                    try:
                        nodeid_data = ",".join(list(str(node_query_dict[x]["_nodeid"]) for x in node_query_dict))
                        cur_dict.execute(stmt.SELECT_CONTAINERINFO_NODEID.format(nodeid_data))
                        result = cur_dict.fetchall()

                        for row in result:
                            podid = row["_podid"]
                            if podid not in pod_container_query_dict:
                                pod_container_query_dict[podid] = list()

                            pod_container_query_dict[podid].append(row)
                    except:
                        pass
                except Exception as e:
                    conn.rollback()
                    self.log.write("GET", f"Kubecontainerinfo has an error. Put data process is stopped. - {str(e)}")
                    return False
                    
                # Check Pod Device(Network, Filesystem) info table
                try:
                    device_type_set = {'network','volume'}
                    device_info = dict()
                    deviceinfo_query_list = dict()

                    for device_type in device_type_set:
                        device_info[device_type] = list()

                        try:
                            cursor.execute(stmt.SELECT_PODDEVICEINFO_DEVICETYPE.format(device_type))
                            deviceinfo_query_list[device_type] = cursor.fetchall()
                        except:
                            deviceinfo_query_list[device_type] = list()

                    netdeviceinfo_dict = dict({x[0]:dict({
                        y["podRef"]["uid"]:list(
                            z["name"] for z in y["network"]["interfaces"]
                        ) for y in x[1] if "network" in y
                    }) for x in kube_data["pod_metric"].items()})

                    for node in netdeviceinfo_dict:
                        for pod in netdeviceinfo_dict[node]:
                            device_info['network'].extend(netdeviceinfo_dict[node][pod])

                    voldeviceinfo_dict = dict({x[0]:dict({
                        y["podRef"]["uid"]:list(
                            z["name"] for z in y["volume"]
                        ) for y in x[1] if "volume" in y
                    }) for x in kube_data["pod_metric"].items()})

                    for node in voldeviceinfo_dict:
                        for pod in voldeviceinfo_dict[node]:
                            device_info['volume'].extend(voldeviceinfo_dict[node][pod])

                    device_info_set = dict({x[0]:set(x[1]) for x in device_info.items()})
                    new_dev_info_set = dict({x:set(filter(lambda y: y not in list(z[1] for z in deviceinfo_query_list[x]), device_info_set[x])) for x in device_type_set})

                    for devtype in device_type_set:
                        for devinfo in new_dev_info_set[devtype]:
                            column_data = self.insert_columns_ref(schema_obj, "kubepoddeviceinfo")
                            value_data = self.insert_values([devinfo, devtype])
                            cursor.execute(stmt.INSERT_TABLE.format("kubepoddeviceinfo", column_data, value_data))
                            conn.commit()
                            self.input_tableinfo("kubepoddeviceinfo", cursor, conn)
                            #self.log.write("PUT", f"Kubepoddeviceinfo insertion is completed - {devtype} / {devinfo}")

                        # New Pod Device info Update
                        try:
                            cur_dict.execute(stmt.SELECT_PODDEVICEINFO_DEVICETYPE.format(devtype))
                            pod_device_query_dict[devtype] = cur_dict.fetchall()
                        except:
                            pass
                except Exception as e:
                    conn.rollback()
                    self.log.write("GET", f"Kubedeviceinfo has an error. Put data process is stopped. - {str(e)}")
                    return False

            # Update Lastrealtimeperf table
            try:
                for node in node_query_dict:
                    node_data = kube_data["node_metric"][node]

                    network_prev_cum_usage = self.system_var.get_network_metric("lastrealtimeperf", node)                
                    network_cum_usage = sum(list(x["rxBytes"]+x["txBytes"] for x in node_data["network"]["interfaces"]))
                    network_usage = network_cum_usage - network_prev_cum_usage if network_prev_cum_usage else 0
                    ontunetime = self.get_ontunetime(cursor)

                    node_perf = [
                        node_query_dict[node]["_nodeid"],
                        ontunetime,
                        self.calculate('cpu_usage_percent', [node_data["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]),
                        self.calculate('memory_used_percent', node_data["memory"]),
                        self.calculate('memory_swap_percent', node_data["memory"]),
                        self.calculate('memory_size', node_data["memory"]),
                        node_data["memory"]["rssBytes"],
                        self.calculate('network', [network_usage]),
                        self.calculate('fs_usage_percent', node_data["fs"]),
                        self.calculate('fs_total_size', node_data["fs"]),
                        self.calculate('fs_inode_usage_percent', node_data["fs"]),
                        self.calculate('fs_usage_percent', node_data["runtime"]["imageFs"]),
                        node_data["rlimit"]["curproc"]
                    ]

                    self.system_var.set_network_metric("lastrealtimeperf", node, network_cum_usage)

                    column_data = self.insert_columns_metric(schema_obj, "kubelastrealtimeperf")
                    value_data = self.insert_values(node_perf)

                    cursor.execute(stmt.DELETE_LASTREALTIMEPERF.format(node_query_dict[node]["_nodeid"]))
                    cursor.execute(stmt.INSERT_TABLE.format("kubelastrealtimeperf", column_data, value_data))
                    conn.commit()
                    self.input_tableinfo("kubelastrealtimeperf", cursor, conn, ontunetime)
                    #self.log.write("PUT", f"Kubelastrealtimeperf update is completed - {node_query_dict[node]['_nodeid']}")
            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kubelastrealtimeperf has an error. Put data process is stopped. - {str(e)}")
                return False

            # Update Realtime table
            ontunetime = self.get_ontunetime(cursor)
            agenttime = self.get_agenttime(cursor)

            try:
                table_postfix = f"_{datetime.now().strftime('%y%m%d')}00"

                for node in node_query_dict:
                    # Setting node data
                    node_data = kube_data["node_metric"][node]
                    nodeid = node_query_dict[node]["_nodeid"]

                    network_prev_cum_usage = self.system_var.get_network_metric("nodeperf", node)
                    network_cum_usage = [
                        self.calculate('network', [sum(list(x["rxBytes"]+x["txBytes"] for x in node_data["network"]["interfaces"]))]),
                        self.calculate('network', [sum(list(x["rxBytes"] for x in node_data["network"]["interfaces"]))]),
                        self.calculate('network', [sum(list(x["txBytes"] for x in node_data["network"]["interfaces"]))]),
                        sum(list(x["rxErrors"] for x in node_data["network"]["interfaces"])),
                        sum(list(x["txErrors"] for x in node_data["network"]["interfaces"]))
                    ]
                    network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                    # Insert nodeperf metric data
                    realtime_nodeperf = [
                        nodeid,
                        ontunetime,
                        agenttime,
                        self.calculate('cpu_usage_percent', [node_data["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]),
                        self.calculate('memory_used_percent', node_data["memory"]),
                        self.calculate('memory_swap_percent', node_data["memory"]),
                        self.calculate('memory_size', node_data["memory"]),
                        node_data["memory"]["rssBytes"]
                    ] + network_usage + [
                        self.calculate('fs_usage_percent', node_data["fs"]),
                        self.calculate('fs_total_size', node_data["fs"]),
                        self.calculate('fs_free_size', node_data["fs"]),
                        self.calculate('fs_inode_usage_percent', node_data["fs"]),
                        self.calculate('fs_inode_total_size', node_data["fs"]),
                        self.calculate('fs_inode_free_size', node_data["fs"]),
                        self.calculate('fs_usage_percent', node_data["runtime"]["imageFs"]),
                        node_data["rlimit"]["maxpid"],
                        node_data["rlimit"]["curproc"]
                    ]

                    self.system_var.set_network_metric("nodeperf", node, network_cum_usage)

                    table_name = f"realtimekubenodeperf{table_postfix}"
                    column_data = self.insert_columns_metric(schema_obj, "kubenodeperf")
                    value_data = self.insert_values(realtime_nodeperf)

                    cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                    conn.commit()
                    self.input_tableinfo(table_name, cursor, conn, ontunetime)
                    #self.log.write("PUT", f"{table_name} update is completed - {node_query_dict[node]['_nodeid']}")

                    # Insert node system container metric data
                    sysco_data = dict({x["name"]: {
                        "cpu": x["cpu"],
                        "memory": x["memory"]
                    } for x in node_data["systemContainers"]})

                    for sysco_query_data in node_sysco_query_dict[nodeid]:
                        containername = sysco_query_data["_containername"]

                        realtime_node_sysco = [
                            nodeid,
                            sysco_query_data["_syscontainerid"],
                            ontunetime,
                            agenttime,
                            self.calculate('cpu_usage_percent', [sysco_data[containername]["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "usageNanoCores" in sysco_data[containername]["cpu"] else 0,
                            self.calculate('memory_used_percent', sysco_data[containername]["memory"]),
                            self.calculate('memory_swap_percent', sysco_data[containername]["memory"]),
                            self.calculate('memory_size', sysco_data[containername]["memory"]),
                            sysco_data[containername]["memory"]["rssBytes"] if "rssBytes" in sysco_data[containername]["memory"] else 0
                        ]

                        table_name = f"realtimekubenodesysco{table_postfix}"
                        column_data = self.insert_columns_metric(schema_obj, "kubenodesysco")
                        value_data = self.insert_values(realtime_node_sysco)

                        cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)

                    #self.log.write("PUT", f"{table_name} update is completed - {node_query_dict[node]['_nodeid']}")

                    # Setting node data
                    pod_data = kube_data["pod_metric"][node]

                    for pod in pod_data:
                        uid = pod["podRef"]["uid"]
                        podid = pod_query_dict[uid]["_podid"]

                        network_prev_cum_usage = self.system_var.get_network_metric("podperf", podid)
                        network_cum_usage = [
                            self.calculate('network', [sum(list(x["rxBytes"]+x["txBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            self.calculate('network', [sum(list(x["rxBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            self.calculate('network', [sum(list(x["txBytes"] for x in pod["network"]["interfaces"]))]) if "network" in pod and "interfaces" in pod["network"] else 0,
                            sum(list(x["rxErrors"] for x in pod["network"]["interfaces"])) if "network" in pod and "interfaces" in pod["network"] else 0,
                            sum(list(x["txErrors"] for x in pod["network"]["interfaces"])) if "network" in pod and "interfaces" in pod["network"] else 0,
                        ]
                        network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                        # Insert pod metric data
                        realtime_podperf = [
                            podid,
                            ontunetime,
                            agenttime,
                            self.calculate('cpu_usage_percent', [pod["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "cpu" in pod and "usageNanoCores" in pod["cpu"] else 0,
                            self.calculate('memory_used_percent', pod["memory"]),
                            self.calculate('memory_swap_percent', pod["memory"]),
                            self.calculate('memory_size', pod["memory"]),
                            pod["memory"]["rssBytes"] if "memory" in pod and "rssBytes" in pod["memory"] else 0
                        ] + network_usage + [
                            sum(int(x["usedBytes"]) for x in pod["volume"]) if "volume" in pod and "usedBytes" in pod["volume"][0] else 0,
                            sum(int(x["inodesUsed"]) for x in pod["volume"]) if "volume" in pod and "inodesUsed" in pod["volume"][0] else 0,
                            pod["ephemeral-storage"]["usedBytes"],
                            pod["ephemeral-storage"]["inodesUsed"],
                            pod["process_stats"]["process_count"] if "ephemeral-storage" in pod and "process_count" in pod["process_stats"] else 0
                        ]

                        self.system_var.set_network_metric("podperf", podid, network_cum_usage)

                        table_name = f"realtimekubepodperf{table_postfix}"
                        column_data = self.insert_columns_metric(schema_obj, "kubepodperf")
                        value_data = self.insert_values(realtime_podperf)

                        cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)
                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

                        # Insert pod container metric data
                        for pod_container in pod["containers"]:
                            realtime_containerperf = [
                                list(filter(lambda x: x["_containername"] == pod_container["name"], list(pod_container_query_dict[podid])))[0]["_containerid"],
                                ontunetime,
                                agenttime,
                                self.calculate('cpu_usage_percent', [pod_container["cpu"]["usageNanoCores"], node_list[node]["cpucount"]]) if "cpu" in pod_container and "usageNanoCores" in pod_container["cpu"] else 0,
                                self.calculate('memory_used_percent', pod_container["memory"]),
                                self.calculate('memory_swap_percent', pod_container["memory"]),
                                self.calculate('memory_size', pod_container["memory"]),
                                pod_container["memory"]["rssBytes"] if "memory" in pod_container and "rssBytes" in pod_container["memory"] else 0,
                                pod_container["rootfs"]["usedBytes"],
                                pod_container["rootfs"]["inodesUsed"],
                                pod_container["logs"]["usedBytes"],
                                pod_container["logs"]["inodesUsed"]
                            ]

                            table_name = f"realtimekubecontainerperf{table_postfix}"
                            column_data = self.insert_columns_metric(schema_obj, "kubecontainerperf")
                            value_data = self.insert_values(realtime_containerperf)

                            cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                            conn.commit()
                            self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

                        # Insert pod device metric data
                        if "network" in pod and "interfaces" in pod["network"]:
                            for pod_network in pod["network"]["interfaces"]:
                                deviceid = list(filter(lambda x: x["_devicename"] == pod_network["name"], list(pod_device_query_dict["network"])))[0]["_deviceid"]
                                podnet_key = f"{podid}_{deviceid}"

                                network_prev_cum_usage = self.system_var.get_network_metric("podnet", podnet_key)
                                network_cum_usage = [
                                    self.calculate('network', [pod_network["rxBytes"] + pod_network["txBytes"]]),
                                    self.calculate('network', [pod_network["rxBytes"]]),
                                    self.calculate('network', [pod_network["txBytes"]]),
                                    pod_network["rxErrors"],
                                    pod_network["txErrors"]                                                        
                                ]
                                network_usage = list(network_cum_usage[x] - network_prev_cum_usage[x] if network_prev_cum_usage else 0 for x in range(len(network_cum_usage)))

                                realtime_podnet = [
                                    podid,
                                    deviceid,
                                    ontunetime,
                                    agenttime
                                ] + network_usage

                                self.system_var.set_network_metric("podnet", podnet_key, network_cum_usage)

                                table_name = f"realtimekubepodnet{table_postfix}"
                                column_data = self.insert_columns_metric(schema_obj, "kubepodnet")
                                value_data = self.insert_values(realtime_podnet)

                                cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                                conn.commit()
                                self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        if "volume" in pod:
                            for pod_volume in pod["volume"]:
                                realtime_podvol = [
                                    podid,
                                    list(filter(lambda x: x["_devicename"] == pod_volume["name"], list(pod_device_query_dict["volume"])))[0]["_deviceid"],
                                    ontunetime,
                                    agenttime,
                                    pod_volume["usedBytes"],
                                    pod_volume["inodesUsed"]
                                ]

                                table_name = f"realtimekubepodvol{table_postfix}"
                                column_data = self.insert_columns_metric(schema_obj, "kubepodvol")
                                value_data = self.insert_values(realtime_podvol)

                                cursor.execute(stmt.INSERT_TABLE.format(table_name, column_data, value_data))
                                conn.commit()
                                self.input_tableinfo(table_name, cursor, conn, ontunetime)

                        #self.log.write("PUT", f"{table_name} update is completed - {uid}")

            except Exception as e:
                conn.rollback()
                self.log.write("GET", f"Kube realtime tables have an error. Put data process is stopped. - {str(e)}")
                return False

            # Update Average table 
            def insert_average_table(table_midfix, key_columns):
                try:
                    today_postfix = f"_{date.today().strftime('%y%m%d')}00"
                    prev_postfix = f"_{(date.today() - timedelta(1)).strftime('%y%m%d')}00"
                    lt_prev_ontunetime = ontunetime - KUBE_MGR_LT_INTERVAL

                    from_clause = str()
                    table_st_prev_name = f"realtime{table_midfix}{prev_postfix}"
                    table_st_name = f"realtime{table_midfix}{today_postfix}"
                    table_lt_name = f"avg{table_midfix}{today_postfix}"

                    # Yesterday table check
                    cursor.execute(stmt.SELECT_PG_TABLES_TABLENAME_COUNT_MET.format(table_midfix, prev_postfix))
                    result = cursor.fetchone()
                    if result[0] > 0:
                        from_clause = f"(select * from {table_st_prev_name} union all select * from {table_st_name}) t"
                    else:
                        from_clause = table_st_name

                    # Between으로 하지 않는 이유는 lt_prev_ontunetime보다 GTE가 아니라 GT가 되어야 하기 때문
                    select_clause = self.select_average_columns(schema_obj, table_midfix, key_columns)
                    where_clause = f"_ontunetime > {lt_prev_ontunetime} and _ontunetime <= {ontunetime}"
                    group_clause = f" group by {','.join(key_columns)}"
                    
                    cursor.execute(stmt.SELECT_TABLE.format(select_clause, from_clause, where_clause, group_clause))
                    result = cursor.fetchall()

                    for row in result:
                        column_data = self.insert_columns_metric(schema_obj, table_midfix)
                        value_data = self.insert_values(list(row[:len(key_columns)]) + [ontunetime, agenttime] + list(row[len(key_columns):]))
                        
                        cursor.execute(stmt.INSERT_TABLE.format(table_lt_name, column_data, value_data))
                        conn.commit()
                        self.input_tableinfo(table_name, cursor, conn, ontunetime)

                    #self.log.write("PUT", f"{table_lt_name} update is completed - {ontunetime}")
                except Exception as e:
                    conn.rollback()
                    self.log.write("GET", f"Kube average tables have an error. Put data process is stopped. - {str(e)}")

            if self.system_var.get_duration() % KUBE_MGR_LT_INTERVAL == 0:
                insert_average_table("kubenodeperf", ["_nodeid"])
                insert_average_table("kubenodesysco", ["_nodeid","_syscontainerid"])
                insert_average_table("kubepodperf", ["_podid"])
                insert_average_table("kubecontainerperf", ["_containerid"])
                insert_average_table("kubepodnet", ["_podid","_deviceid"])
                insert_average_table("kubepodvol", ["_podid","_deviceid"])
