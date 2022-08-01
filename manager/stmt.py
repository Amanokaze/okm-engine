SELECT_PG_TABLES_TABLENAME_COUNT_REF = "select count(*) from pg_tables where tablename = '{}'"
SELECT_PG_TABLES_TABLENAME_COUNT_MET = "select count(*) from pg_tables where tablename in ('realtime{0}{1}','avg{0}{1}');"

SELECT_ONTUNEINFO = "select _time from ontuneinfo limit 1"
SELECT_ONTUNEINFO_BIAS = "select _time, _bias from ontuneinfo;"

SELECT_TABLEINFO_TABLENAME = "select count(*) from kubetableinfo where _tablename='{}';"
UPDATE_TABLEINFO = "update kubetableinfo set _updatetime={} where _tablename='{}';"
INSERT_TABLEINFO = "insert into kubetableinfo values ('{1}', 0, {0}, {0}, 0);"

INSERT_TABLE = "insert into {} ({}) values ({});"
SELECT_TABLE = "select {} from {} where {} {};"
UPDATE_ENABLED = "update {} set _enabled=0 where {} in ({});"

UPDATE_NODEINFO = "update kubenodeinfo set {} where _nodeid = {};"
DELETE_LASTREALTIMEPERF = "delete from kubelastrealtimeperf where _nodeid={};"

SELECT_MANAGERINFO_IP = "select * from kubemanagerinfo where _ip='{}';"
SELECT_CLUSTERINFO_IP_MGRID = "select * from kubeclusterinfo where _ip='{}' and _managerid={};"
SELECT_NAMESPACEINFO_CLUSTERID = "select * from kubensinfo where _clusterid={} and _enabled=1;"
SELECT_NODEINFO_CLUSTERID = "select * from kubenodeinfo where _clusterid={} and _enabled=1;"
SELECT_NODE_SYSCONTAINER_NODEID = "select nsc.*, n._nodename from kubenodesyscoinfo nsc, kubenodeinfo n where nsc._nodeid=n._nodeid and n._nodeid in ({});"
SELECT_PODINFO_NODEID = "select * from kubepodinfo where _nodeid in ({}) and _enabled=1;"
SELECT_CONTAINERINFO_NODEID = "select c.*, p._uid as _poduid, n._nodename from kubecontainerinfo c, kubepodinfo p, kubenodeinfo n where c._podid=p._podid and p._nodeid=n._nodeid and p._enabled=1 and p._nodeid in ({});"
SELECT_PODDEVICEINFO_DEVICETYPE = "select * from kubepoddeviceinfo where _devicetype='{}';"

SELECT_NODEINFO_MGRIP = "select n.* from kubenodeinfo n, kubeclusterinfo c, kubemanagerinfo m where m._managerid = c._managerid and c._clusterid = n._clusterid and m._managerid = n._managerid and n._enabled=1 and m._ip='{}';"

SELECT_VIEWER_OVERALL = """SELECT n._nodename, p.*
  FROM kubelastrealtimeperf p, kubenodeinfo n
 WHERE n._nodeid = p._nodeid and n._nodeid in ({})
 ORDER BY n._nodename;"""

SELECT_VIEWER_NODESYSCO = """SELECT n._nodename, nsc._containername, rnsc.*
  FROM realtimekubenodesysco_{0} rnsc, kubenodeinfo n, kubenodesyscoinfo nsc,
       (select _updatetime from kubetableinfo where _tablename='realtimekubenodesysco_{0}') t
 WHERE n._nodeid = rnsc._nodeid and n._nodeid = {1}
   AND rnsc._ontunetime = t._updatetime
   and rnsc._syscontainerid = nsc._syscontainerid
 ORDER BY nsc._containername;"""

SELECT_VIEWER_POD = """SELECT p._podname, rp.*
  FROM realtimekubepodperf_{0} rp, kubepodinfo p,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodperf_{0}') t
 WHERE p._podid = rp._podid and p._nodeid = {1}
   AND rp._ontunetime = t._updatetime
 ORDER BY p._podid;"""

SELECT_VIEWER_CONTAINER = """SELECT c._containername, rcp.*
  FROM realtimekubecontainerperf_{0} rcp, kubecontainerinfo c,
       (select _updatetime from kubetableinfo where _tablename='realtimekubecontainerperf_{0}') t
 WHERE c._containerid = rcp._containerid and c._podid = {1}
   AND rcp._ontunetime = t._updatetime
 ORDER BY c._podid;"""

SELECT_VIEWER_PODNET = """SELECT d._devicename, rpn.*
  FROM realtimekubepodnet_{0} rpn, kubepoddeviceinfo d,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodnet_{0}') t
 WHERE d._deviceid = rpn._deviceid and rpn._podid = {1}
   AND rpn._ontunetime = t._updatetime
 ORDER BY d._devicename;"""

SELECT_VIEWER_PODVOL = """SELECT d._devicename, rpv.*
  FROM realtimekubepodvol_{0} rpv, kubepoddeviceinfo d,
       (select _updatetime from kubetableinfo where _tablename='realtimekubepodvol_{0}') t
 WHERE d._deviceid = rpv._deviceid and rpv._podid = {1}
   AND rpv._ontunetime = t._updatetime
 ORDER BY d._devicename;"""

SELECT_VIEWER_PODTS = """SELECT 
        p._podname, t._ontunetime, t._agenttime, t._cpuusage, t._memoryused
  FROM (SELECT m.* FROM realtimekubepodperf_{0} m, kubepodinfo i where m._podid=i._podid and i._nodeid={2} and m._ontunetime>={3}
        UNION ALL
        SELECT m.* FROM realtimekubepodperf_{1} m, kubepodinfo i where m._podid=i._podid and i._nodeid={2} and m._ontunetime>={3}) t,
       kubepodinfo p
 WHERE p._podid = t._podid
 ORDER BY t._podid, t._ontunetime"""

SELECT_VIEWER_PODTS2 = """SELECT 
        p._podname, t._ontunetime, t._agenttime, t._cpuusage, t._memoryused
  FROM (SELECT m.* FROM realtimekubepodperf_{0} m, kubepodinfo i where m._podid=i._podid and i._nodeid={1} and m._ontunetime>={2}) t,
       kubepodinfo p
 WHERE p._podid = t._podid
 ORDER BY t._podid, t._ontunetime"""