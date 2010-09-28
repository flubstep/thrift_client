#!/usr/bin/env python

import logging

_DEFAULT_TIMEOUT = 60001

def _canonicalize_hostport(host, port):
    if port is not None:
        return host, port
    elif port is None and ':' in host:
        host, port = host.split(':')
        port = int(port)
        return host, port
    else:
        raise ValueError('Invalid host, port pair: %r', (host, port))

class ClientDisabledError(Error):
    pass
        
class SimpleClient():
    def __init__(self, protocol, host, port, frame=False, log_filename=None, timeout=None):
        self.protocol = protocol
        self.host, self.port = _canonicalize_hostport(host, port)
        self.frame = frame
        self.timeout = timeout or _DEFAULT_TIMEOUT
        self.file = None
        self.enabled = True
        if log_filename:
            self.file = open(log_filename, 'ab')
        
    def enable(self):
        self.enabled = True
        
    def disable(self):
        self.enabled = False
        
    def is_enabled(self):
        return self.enabled
        
    def _connect(self):
        self.socket = TSocket.TSocket(self.host, self.port)
        self.socket.setTimeout(self.timeout)
        transport = TTransport.TBufferedTransport(self.socket)
        if self.frame:
            transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = self.protocol.Client(protocol)
        transport.open()
        return client
        
    def _connect_file(self):
        transport = TTransport.TFileObjectTransport(self.file)
        if self.frame:
            transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = self.protocol.Client(iprot=TProtocol.TProtocolBase(transport), oprot=protocol)
        transport.open()
        return client
    
    def __getattr__(self, k):
        def f(*args, **kwargs):
            if not self.is_enabled():
                raise ClientDisabledError()
            if self.file:
                client_file = self._connect_file()
            try:
                getattr(client_file, k)(*args, **kwargs)
            except:
                pass # Errors are throw after writing, simply ignore them
            
            client = self._connect()
            ret = getattr(client, k)(*args, **kwargs)
            self.socket.close()
            return ret
            
        return f
        
    def __str__(self):
        return '<%s client %s:%d>' % (self.protocol.__name__, self.host, self.port)
        
class ReplicatedClient():
    def __init__(self, protocol, frame=False, timeout=None):
        self.protocol = protocol
        self.frame = frame
        self.timeout = None

        self.servers = []
        
    def add_server(self, host=None, port=None, server=None):
        if not server:
            server = SimpleClient(self.protocol, host, port, self.frame, None, self.timeout)
        self.servers.append(server)
        return self
        
    def remove_server(self, server=None, host=None, port=None):
        if server:
            self.servers.remove(server)
        else:
            host, port = _canonicalize_hostport(host, port)
            self.servers = [s for s in self.servers if (host, port) != (s.host, s.port)]
        return self
        
    def __getattr__(self, k):
        def f(*args, **kwargs):
            responses = []
            for server in self.servers:
                try:
                    response = getattr(server, k)(*args, **kwargs)
                except Exception e:
                    response = e
                responses.append((server, response))
            return responses
        return f
        
    def __str__(self):
        return '<replicated %r>' % self.servers
        
class ThreadedReplicatedClient(ReplicatedClient):
    def __getattr__(self, k):
        def f(*args, **kwargs):
            response_for = {}
            def get_response(server):
                try:
                    response = getattr(server, k)(*args, **kwargs)
                except Exception, e:
                    response = e
                response_for[server] = response
                
            threads = []
            for server in servers:
                threads.append(threading.Thread(target=get_response, args=(server,)))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            return response_for.items()
            
        return f

    def __str__(self):
        return '<threaded replicated %r>' % self.servers

class HashClient():
    def __init__(self, protocol, frame=False, log_filename=None, timeout=None):
        self.servers = []
        self.protocol = protocol
        self.frame = frame
        self.log_filename = log_filename
        self.timeout = timeout
        
        self.all = ReplicatedClient(protocol, frame, log_filename)
        self.hashfns = {}

    def add_server(self, host=None, port=None, server=None):
        if not server:
            server = SimpleClient(self.protocol, host, port, self.frame, self.log_filename, timeout)
        self.servers.append(server)
        self.all.add_server(server=server)
        return self

    def remove_server(self, server=None, host=None, port=None):
        ReplicatedClient.remove_server(self, server, host, port)
        self.all.remove_server(server, host, port)
        return self
    
    def set_hash(self, fnname, hashfn):
        self.hashfuncs[fnname] = hashfn
        return self
        
    def __getattr__(self, k):
        def f(*args, **kwargs):
            if k in self.hashfns:
                hashval = self.hashfns[k](*args, **kwargs)
            else:
                hashkey = tuple(args + sorted(kwargs.items()))
            hashval = hash(hashkey)
            server_index = hashval % len(self.servers)
            server = self.servers[server_index]
            try:
                return getattr(server, k)(*args, **kwargs)
            except Exception, e:
                e.server = server
                raise e
        return f
        
    def __str__(self):
        return '<hash client %r>' % self.servers
    
class ThreadedHashClient(HashClient):
    def __init__(self, protocol, frame=False, log_filename=None, timeout=None):
        HashClient.__init__(self, protocol, frame, log_filename, timeout)
        self.all = ThreadedReplicatedClient(protocol, frame, log_filename, timeout)
        
    def __str__(self):
        return '<threaded hash client %r>' % self.servers
    
"""
Usage notes:

typ_pool = ThreadedHashMultiClient(typersearch_if)

>>> typ_pool.servers
[]
>>> typ_pool.add_server(host='localhost:6233')
>>> typ_pool.add_server(host='localhost:6234')
>>> typ_pool.servers
[typersearch_if(localhost:6233), typersearch_if(localhost:6234)]


try:
    typ_pool.search('term')
except Exception, e:
    log.error('Server %r raised an exception during search()' % e.server)
    raise e

def error_handler(server, e):
    server.disable()
    log.error('Received exception from %r: %r' % (server, e))

# Processing return values from a multi-server Thrift call.
for server, response in typ_pool.all.ping():
    if response != 'pong':
        log.error('Received invalid pong from server: %r' % server)
        typ_pool.remove_server(server)

# Sending a call to all Thrift servers.
typ_pool.all.add_document(document)

# Processing errors 
typ_pool.all.ping.set_error_handler(error_handler)

"""