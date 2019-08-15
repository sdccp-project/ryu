# Copyright (C) 2016 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from operator import attrgetter

from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub

import json

BOTTLENECK_BANDWIDTH_BytesPS = 490000.0

import re
from subprocess import *

INTERVAL_S = 0.5      # The period of sending stat request.
INTERFACE = 'r1-eth2'
USE_EWMA = False


class SimpleMonitor13(simple_switch_13.SimpleSwitch13):

    def __init__(self, *args, **kwargs):
        super(SimpleMonitor13, self).__init__(*args, **kwargs)
        self.datapaths = {}
        self.monitor_thread = hub.spawn(self._monitor)
        self.last_rx_bytes = 0
        self.link_utilization = 0.0
        self.users_byte_count = {}
        self.users_utilization = {}

    @set_ev_cls(ofp_event.EventOFPStateChange,
                [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if datapath.id not in self.datapaths:
                self.logger.debug('register datapath: %016x', datapath.id)
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                self.logger.debug('unregister datapath: %016x', datapath.id)
                del self.datapaths[datapath.id]

    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(INTERVAL_S)

    def _request_stats(self, datapath):
        self.logger.debug('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        # self.logger.info('%s', json.dumps(ev.msg.to_jsondict(), ensure_ascii=True,
        #                                   indent=3, sort_keys=True))
        #
        self.logger.info('datapath         '
                         'in-port  eth-src           '
                         'eth-dst           '
                         'out-port packets  bytes')
        self.logger.info('---------------- '
                         '-------- ----------------- '
                         '----------------- '
                         '-------- -------- --------')
        users_bytes_increment = {}
        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            # self.logger.info('eth_src: %s', stat.match['eth_src'])
            self.logger.info('%016x %8x %17s %17s %8x %8d %8d',
                             ev.msg.datapath.id,
                             stat.match['in_port'],
                             stat.match['eth_src'],
                             stat.match['eth_dst'],
                             stat.instructions[0].actions[0].port,
                             stat.packet_count, stat.byte_count)
            if ev.msg.datapath.id == 0x1 and \
                    stat.instructions[0].actions[0].port == 3:
                eth_src = stat.match['eth_src']
                pre_byte_count = self.users_byte_count.get(eth_src, 0)
                byte_count = stat.byte_count
                users_bytes_increment[eth_src] = byte_count - pre_byte_count
                self.users_byte_count[eth_src] = stat.byte_count
        count_of_users = len(users_bytes_increment)
        if count_of_users:
            for user, bytes_increment in users_bytes_increment.items():
                self.users_utilization[user] = bytes_increment / BOTTLENECK_BANDWIDTH_BytesPS / INTERVAL_S * count_of_users
        self.logger.info('users utilization: %s', str(self.users_utilization))

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        # self.logger.info('%s', json.dumps(ev.msg.to_jsondict(), ensure_ascii=True,
        #                                   indent=3, sort_keys=True))

        # self.logger.info('datapath         port     '
        #                  'rx-pkts  rx-bytes rx-error '
        #                  'tx-pkts  tx-bytes tx-error')
        # self.logger.info('---------------- -------- '
        #                  '-------- -------- -------- '
        #                  '-------- -------- --------')
        for stat in sorted(body, key=attrgetter('port_no')):
            # self.logger.info('%016x %8x %8d %8d %8d %8d %8d %8d',
            #                  ev.msg.datapath.id, stat.port_no,
            #                  stat.rx_packets, stat.rx_bytes, stat.rx_errors,
            #                  stat.tx_packets, stat.tx_bytes, stat.tx_errors)

            if (ev.msg.datapath.id == 2) and (stat.port_no == 0x2):
                last_rx_bytes = 0
                if USE_EWMA:
                    last_rx_bytes = 0.2 * self.last_rx_bytes + 0.8 * stat.rx_bytes
                    incremental_rx_bytes = last_rx_bytes - self.last_rx_bytes
                else:
                    last_rx_bytes = stat.rx_bytes
                    incremental_rx_bytes = stat.rx_bytes - self.last_rx_bytes
                self.last_rx_bytes = last_rx_bytes
                self.link_utilization = float(incremental_rx_bytes) / BOTTLENECK_BANDWIDTH_BytesPS / INTERVAL_S
                self.logger.info("==========incremental rx-bytes in the past %.1fs: %d, link utilization: %f=========",
                                 INTERVAL_S, incremental_rx_bytes, self.link_utilization)

    def get_utilization(self, user=None):
        queue_length = QueueMonitor.get_queue_size()
        link_utilization = self.users_utilization.get(user, 0) if user else self.link_utilization
        return link_utilization, queue_length


class QueueMonitor(object):
    @staticmethod
    def get_queue_size():
        pat_queued = re.compile(r'backlog\s(\d+k?)b')
        cmd = "tc -s qdisc show dev %s" % INTERFACE
        p = Popen(cmd, shell=True, stdout=PIPE)
        output = p.stdout.read()
        # Not quite right, but will do for now
        matches = pat_queued.findall(output)
        if matches and len(matches) > 1:
            res = matches[1]
            if res.endswith('k'):
                res = int(res[:-1]) * 1000
            else:
                res = int(res)
            return res
        return -1

