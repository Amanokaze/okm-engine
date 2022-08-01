import stmt
from datetime import datetime
from flask import Flask, render_template, make_response, jsonify
from flask_restful import Api, Resource
from db import DB

TZ_INTERVAL = 60 * 60 * 9

app = Flask(__name__)
api = Api(app)
db = DB(True)

@app.template_filter('strftime')
def _jinja2_filter_strftime(ts):
    return datetime.fromtimestamp(ts - TZ_INTERVAL).strftime('%Y-%m-%d %H:%M:%S')

@app.context_processor
def calculate_processor():
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
    return dict(divide=divide)

class Overall(Resource):
    def get(self):
        try:
            with db.get_resource_rdb() as (cursor, cur_dict, _):
                manager_ip = db.get_basic_info("host")
                cursor.execute(stmt.SELECT_NODEINFO_MGRIP.format(manager_ip))
                node_ids = list(x[0] for x in cursor.fetchall())
                node_ids_str = list(str(x) for x in node_ids)

                cur_dict.execute(stmt.SELECT_TABLE.format("n._nodename, p.*", "kubelastrealtimeperf p, kubenodeinfo n", f"n._nodeid = p._nodeid and n._nodeid in ({','.join(node_ids_str)})", ""))
                return jsonify(list(dict(x) for x in cur_dict.fetchall()))
        except Exception as e:
            return jsonify({'error':str(e)})

class Main(Resource):
    def get(self):
        return make_response(render_template('index.html'))

api.add_resource(Main, '/')
api.add_resource(Overall, '/data/overall')


if __name__=="__main__":
    app.run(debug=True)