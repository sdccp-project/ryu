from simple_monitor_13 import SimpleMonitor13
from ryu.app.wsgi import WSGIApplication, Response, ControllerBase, route
import json


class MonitorAPI(SimpleMonitor13):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(MonitorAPI, self).__init__(*args, **kwargs)
        wsgi = kwargs['wsgi']
        wsgi.register(MonitorController, {'monitor_api_app': self})


class MonitorController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(MonitorController, self).__init__(req, link, data, **config)
        self.topology_api_app = data['monitor_api_app']

    @route('monitor', '/test', methods=['GET'])
    def test(self, req, **kwargs):
        data = [ {'test': 0} ]
        body = json.dumps(data)
        return Response(content_type='application/json', body=body)

    @route('monitor', '/get_link_utilization', methods=['GET'])
    def get_utilization(self, req, **kwargs):
        data = [{'link_utilization': self.topology_api_app.get_utilization()}]
        body = json.dumps(data)
        return Response(content_type='application/json', body=body)
