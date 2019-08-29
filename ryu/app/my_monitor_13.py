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
        data = [{'test': 0}]
        body = json.dumps(data)
        print(type(req))
        print(str(req.environ))
        return Response(content_type='application/json', body=body)

    @route('monitor', '/get_link_utilization', methods=['GET'])
    def get_utilization(self, req, **kwargs):
        utilization, queue_length = self.topology_api_app.get_utilization()
        data = {'link_utilization': utilization,
                'queue_length': queue_length}
        body = json.dumps(data)
        return Response(content_type='application/json', body=body)

    @route('monitor', '/get_user_link_utilization/{user_eth}',
           methods=['GET'])
    def get_user_utilization(self, req, user_eth, **kwargs):
        def int_to_eth(i):
            return "00:00:00:00:00:%s" % str(int(i)).zfill(2)
        user_eth = int_to_eth(user_eth)
        utilization, queue_length = self.topology_api_app.get_utilization(user=user_eth)
        data = {'link_utilization': utilization,
                'queue_length': queue_length}
        body = json.dumps(data)
        return Response(content_type='application/json', body=body)

    @route('monitor', '/set_bottleneck_capacity_Bps',
           methods=['PUT'])
    def set_bottleneck_capacity_Bps(self, req, **kwargs):
        try:
            rest = req.json if req.body else {}
        except ValueError:
            print('invalid syntax %s', req.body)
            return Response(status=400)
        print(rest)
        capacity = int(rest['bottleneck_capacity_Bps'])
        self.topology_api_app.set_bottleneck_capacity_Bps(capacity)

