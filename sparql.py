#most of this is from gevent-websocket example!
# its a dirty proof of concept to demonstrate result streaming to a client via websocket
import traceback
from json import dumps, loads
import time
import ConfigParser

from kyoto_cabinet_stringstore import *
import index_manager as im
import query_engine
import logging 
from index_manager import *
from lubm_queries2 import *

class Application(object):

    def __init__(self):
        print "init app"
        #self.buffer = []
        LOG_FILENAME = 'logs/websocket-server.log'
        logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR)   
        self.config = ConfigParser.RawConfigParser()
        self.config.read("cfgs/benchmark1-kc-kc.cfg") 
        logging.debug("starting stringstore")
        self.stringstore = KyotoCabinetStringstore(self.config)
        globals()["stringstore"] = self.stringstore
        logging.debug("starting index_manager")
        self.iman = im.IndexManager(self.config) 
        globals()["iman"] = self.iman 
        logging.debug("starting QueryEngine")  
        self.qe = query_engine.QueryEngine(self.stringstore, self.iman, self.config)
        self.users = set()
        print "init done"
    
    def __del__(self):
        print "closing stringstore & iman"
        self.stringstore.close()
        self.iman.close()
        print "done"
        
    def __call__(self, environ, start_response):
        print "CALL!!"
        socket = environ.get('websocket')
        if socket is not None:
            self.handle_websocket(socket)
        else:
            path = environ['PATH_INFO'].strip('/')
            if not path:
                start_response('200 OK', [('Content-Type', 'text/html')])
                return ['<h1>Welcome. Try the <a href="/sparql.html">chat</a> example.</h1>']

            if path in ['json.js', 'sparql.html']:
                try:
                    data = open(path).read().replace('$PORT', str(PORT))
                except Exception:
                    traceback.print_exc()
                    return not_found(start_response)
                start_response('200 OK', [('Content-Type', 'text/javascript' if path.endswith('.js') else 'text/html'),
                                          ('Content-Length', str(len(data)))])
                return [data]
        return not_found(start_response)

    def handle_websocket(self, socket):
        #socket.send(dumps({'buffer': self.buffer}))
        socket.sessionid = '%s:%s' % socket.getpeername()
        announcement = '%s connected' % socket.sessionid
        self.broadcast(dumps({'announcement': announcement}), socket.sessionid)
        self.users.add(socket)
        try:
            while True:
                message = socket.receive()
                if message is None:
                    announcement = '%s disconnected' % socket.sessionid
                    self.broadcast(dumps({'announcement': announcement}), socket.sessionid)
                    break
                else:
                    #message = {'message': [socket.sessionid, message], "query":message}
                    #self.buffer.append(message)
                    #if len(self.buffer) > 15:
                    #    del self.buffer[0]
                    self.broadcast(dumps(message), socket.sessionid)                                             
        finally:
            self.users.discard(socket)

    def broadcast(self, message, me=None): 
        print str(loads(message))
        for socket in self.users:            
            if socket.sessionid == me:
                socket.send(dumps({'status': "resolving query..."})) 
                nr_of_results = 0  

                for res in self.qe.execute(loads(message)):
                                    nr_of_results += 1
                                    if (nr_of_results% 10 == 0): 
                                        socket.send(dumps({'status': "resolving query: (%i results so far)" % nr_of_results}))
                                    socket.send( dumps( {"result": res}))                                                   
                socket.send(dumps({'status': "query finished with %i results" % nr_of_results}))
            if socket.sessionid != me:
                try: 
                    pass
                    #socket.send(message)
                except IOError:
                    self.users.discard(message)


def not_found(start_response):
    start_response('404 Not Found', [])
    return ['<h1>Not Found</h1>']


if __name__ == '__main__':
    from websocket.server import WebsocketServer
    PORT = 8000
    try:
        WebsocketServer(('', PORT), Application()).serve_forever()
    except KeyboardInterrupt:
        print "closing down..." 
        stringstore.close()
        iman.close()
        print "done!"  
    
