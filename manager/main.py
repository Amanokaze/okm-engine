import stmt
from alpha_engine import Engine
from datetime import datetime, date, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db import DB

app = FastAPI()
engine = Engine()
db = DB(True)
print(app)
origins = [
    "http://localhost"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

def divide(value, unit, digit=0):
    if type(unit) == int:
        return round(value / unit, digit)
    elif unit == "kb":
        return round(value / 1024, digit)
    elif unit == "mb":
        return round(value / 1024 / 1024, digit)
    elif unit == "gb":
        return round(value / 1024 / 1024 / 1024, digit)
    else:
        return round(value, digit)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/data/overall")
def overall():
    with db.get_resource_rdb() as (cursor, cur_dict, _):
        manager_ip = db.get_basic_info("host")
        cursor.execute(stmt.SELECT_NODEINFO_MGRIP.format(manager_ip))
        node_ids = list(x[0] for x in cursor.fetchall())
        node_ids_str = list(str(x) for x in node_ids)

        cur_dict.execute(stmt.SELECT_VIEWER_OVERALL.format(','.join(node_ids_str)))
        return list({
            "_nodename": x["_nodename"],
            "_nodeid": x["_nodeid"],
            "_ontunetime": x["_ontunetime"],
            "_cpuusage": x["_cpuusage"],
            "_memoryused": divide(x["_memoryused"], 100, 2),
            "_swapused": divide(x["_swapused"], 100, 2),
            "_memorysize": divide(x["_memorysize"], "gb", 2),
            "_swapsize": divide(x["_swapsize"], "gb", 2),
            "_netusage": divide(x["_netusage"], 100, 2),
            "_fsusage": x["_fsusage"],
            "_fssize": divide(x["_fssize"], "kb", 2),
            "_fsiusage": x["_fsiusage"],
            "_imgfsusage": x["_imgfsusage"],
            "_proccount": x["_proccount"]
        } for x in cur_dict.fetchall())

@app.get("/data/nodesysco/{nodeid}")
def nodesysco(nodeid):
    with db.get_resource_rdb() as (_, cur_dict, _):
        table_postfix = f"{datetime.now().strftime('%y%m%d')}00"        
        
        cur_dict.execute(stmt.SELECT_VIEWER_NODESYSCO.format(table_postfix, nodeid))
        return list({
            "_nodename": x["_nodename"],
            "_containername": x["_containername"],
            "_nodeid": x["_nodeid"],
            "_ontunetime": x["_ontunetime"],
            "_cpuusage": x["_cpuusage"],
            "_memoryused": divide(x["_memoryused"], 100, 2),
            "_swapused": divide(x["_swapused"], 100, 2),
            "_memorysize": divide(x["_memorysize"], "gb", 2),
            "_swapsize": divide(x["_swapsize"], "gb", 2),
        } for x in cur_dict.fetchall())

@app.get("/data/pod/{nodeid}")
def pod(nodeid):
    with db.get_resource_rdb() as (_, cur_dict, _):
        table_postfix = f"{datetime.now().strftime('%y%m%d')}00"        
        
        cur_dict.execute(stmt.SELECT_VIEWER_POD.format(table_postfix, nodeid))
        return list({
            "_podname": x["_podname"],
            "_podid": x["_podid"],
            "_ontunetime": x["_ontunetime"],
            "_cpuusage": x["_cpuusage"],
            "_memoryused": divide(x["_memoryused"], 100, 2),
            "_swapused": divide(x["_swapused"], 100, 2),
            "_memorysize": divide(x["_memorysize"], "gb", 2),
            "_swapsize": divide(x["_swapsize"], "gb", 2),
            "_netusage": divide(x["_netusage"], 100, 2),
            "_netrxrate": divide(x["_netrxrate"], 100, 2),
            "_nettxrate": divide(x["_nettxrate"], 100, 2),
            "_netrxerrors": x["_netrxerrors"],
            "_nettxerrors": x["_nettxerrors"],
            "_volused": x["_volused"],
            "_voliused": x["_voliused"],
            "_epstused": x["_epstused"],
            "_epstiused": x["_epstiused"],
            "_proccount": x["_proccount"]
        } for x in cur_dict.fetchall())

@app.get("/data/container/{podid}")
def container(podid):
    with db.get_resource_rdb() as (_, cur_dict, _):
        table_postfix = f"{datetime.now().strftime('%y%m%d')}00"        
        
        cur_dict.execute(stmt.SELECT_VIEWER_CONTAINER.format(table_postfix, podid))
        return list({
            "_containername": x["_containername"],
            "_containerid": x["_containerid"],
            "_ontunetime": x["_ontunetime"],
            "_cpuusage": x["_cpuusage"],
            "_memoryused": divide(x["_memoryused"], 100, 2),
            "_swapused": divide(x["_swapused"], 100, 2),
            "_memorysize": divide(x["_memorysize"], "gb", 2),
            "_swapsize": divide(x["_swapsize"], "gb", 2),
            "_rootfsused": x["_rootfsused"],
            "_rootfsiused": x["_rootfsiused"],
            "_logfsused": x["_logfsused"],
            "_logfsiused": x["_logfsiused"]
        } for x in cur_dict.fetchall())

@app.get("/data/podnet/{podid}")
def podnet(podid):
    with db.get_resource_rdb() as (_, cur_dict, _):
        table_postfix = f"{datetime.now().strftime('%y%m%d')}00"        
        
        cur_dict.execute(stmt.SELECT_VIEWER_PODNET.format(table_postfix, podid))
        return list({
            "_devicename": x["_devicename"],
            "_ontunetime": x["_ontunetime"],
            "_netusage": divide(x["_netusage"], 100, 2),
            "_netrxrate": divide(x["_netrxrate"], 100, 2),
            "_nettxrate": divide(x["_nettxrate"], 100, 2),
            "_netrxerrors": x["_netrxerrors"],
            "_nettxerrors": x["_nettxerrors"]
        } for x in cur_dict.fetchall())

@app.get("/data/podvol/{podid}")
def podnet(podid):
    with db.get_resource_rdb() as (_, cur_dict, _):
        table_postfix = f"{datetime.now().strftime('%y%m%d')}00"        
        
        cur_dict.execute(stmt.SELECT_VIEWER_PODVOL.format(table_postfix, podid))
        return list({
            "_devicename": x["_devicename"],
            "_ontunetime": x["_ontunetime"],
            "_volused": x["_volused"],
            "_voliused": x["_voliused"]
        } for x in cur_dict.fetchall())

@app.get("/data/podts/{nodeid}")
def pod(nodeid):
    with db.get_resource_rdb() as (cursor, cur_dict, _):
        CHART_INTERVAL = 600
        today_postfix = f"{date.today().strftime('%y%m%d')}00"
        prev_postfix = f"{(date.today() - timedelta(1)).strftime('%y%m%d')}00"

        cursor.execute(stmt.SELECT_ONTUNEINFO)
        curr_ontunetime = cursor.fetchone()[0]
        prev_ontunetime = curr_ontunetime - CHART_INTERVAL

        cursor.execute(stmt.SELECT_TABLEINFO_TABLENAME.format(f'realtimekubepodperf_{prev_postfix}'))
        result = cursor.fetchone()[0]
        exec_statement = stmt.SELECT_VIEWER_PODTS.format(today_postfix, prev_postfix, nodeid, prev_ontunetime) if result > 0 else stmt.SELECT_VIEWER_PODTS2.format(today_postfix, nodeid, prev_ontunetime)
        cur_dict.execute(exec_statement)

        return list({
            "_podname": x["_podname"],
            "_ontunetime": x["_ontunetime"],
            "_agenttime": x["_agenttime"],
            "_cpuusage": x["_cpuusage"],
            "_memoryused": divide(x["_memoryused"], 100, 2)
        } for x in cur_dict.fetchall())        