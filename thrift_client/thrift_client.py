__doc__ = """Classes for common use cases of Thrift clients."""
__author__ = """Albert Sheu"""

import logging
import random
import socket
import threading

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol, TProtocol

_DEFAULT_TIMEOUT = 60001

def _canonicalize_hostport(host, port):
    """Turn instances of hostname:port into two separate objects,
    string of the host, and int of the port. Valid host, port arguments
    are returned. Idempotent function."""
    if port is not None:
        return host, port
    elif port is None and ':' in host:
        host, port = host.split(':')
        port = int(port)
        return host, port
    else:
        raise ValueError('Invalid host, port pair: %r', (host, port))

class ClientDisabledError(Exception):
    """Raised when a function call is attempted on a disabled client."""
    pass

class ThriftResponse():
    """Object representing a return value by a Thrift server. Includes
    the client object that made the request, as well as the object
    returned by the thrift call."""
    def __init__(self, server, response):
        self.server = server
        self.response = response

    def value(self):
        """Returns the value returned by the Thrift server."""
        return self.response

    def is_error(self):
        """Returns whether an exception was raised by the RPC."""
        return False

    def __str__(self):
        return '<thrift response: %r>' % self.response

    def __repr__(self):
        return str(self)

class ThriftExceptionResponse(ThriftResponse):
    """Object representing a Thrift response where an exception was
    raised, either by the Thrift server or by the underlying transport.
    Calling .value() on an ExceptionResponse will raise the exception
    originally raised."""
    def __init__(self, server, exception):
        self.server = server
        self.exception = exception

    def value(self):
        """Does not return, instead raises the exception raised by the server."""
        self.exception.server = self.server
        raise self.exception

    def is_error(self):
        """Returns whether an exception was raised by the RPC."""
        return True

    def __str__(self):
        return '<thrift exception object>'

class SimpleClient(threading.local):
    """Returns a new instance of the SimpleClient class. Represents a connection
    to a single Thrift server of the given protocol. host and port represent the
    location of the Thrift service. frame indicates whether the service is
    running under a TFramedTransport. timeout indicates the socket timeout
    factor on the socket connect(), send(), and recv()."""
    def __init__(self, protocol, host, port=None, frame=False, log_filename=None, timeout=None, name=None):
        self.protocol = protocol
        self.host, self.port = _canonicalize_hostport(host, port)
        self.frame = frame
        self.timeout = timeout or _DEFAULT_TIMEOUT
        self.file = None
        self.enabled = True
        self.name = name or '%s-%d' % (self.protocol.__name__, id(self))
        if log_filename:
            self.file = open(log_filename, 'ab')

    def enable(self):
        """Allows function calls to be made through the Thrift client."""
        self.enabled = True

    def disable(self):
        """Causes client to raise a ClientDisabledError when attempting to
        call a remote function using this client."""
        self.enabled = False

    def is_enabled(self):
        """Returns whether the client has been enabled or not."""
        return self.enabled

    def _connect(self):
        """Initializes the socket, transport, protocol, and session for
        the Thrift service."""
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
        """Initializes the transport of the Thrift service to write to a
        logfile instead of a socket transport."""
        transport = TTransport.TFileObjectTransport(self.file)
        if self.frame:
            transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = self.protocol.Client(iprot=TProtocol.TProtocolBase(transport), oprot=protocol)
        transport.open()
        return client

    def __getattr__(self, k):
        """Proxy function for executing Thrift calls. Client will initialize
        the connection to the remote host or file, execute the command, and
        then close the socket. Because the socket is created on every request,
        every individual request is thread-safe (on the client level.) Raises
        an error on transport or Thrift errors."""
        def f(*args, **kwargs):
            if not self.is_enabled():
                raise ClientDisabledError()
            if self.file:
                client_file = self._connect_file()
            try:
                getattr(client_file, k)(*args, **kwargs)
            except:
                pass # Errors are throwm after writing, simply ignore them.

            client = self._connect()
            ret = getattr(client, k)(*args, **kwargs)
            self.socket.close()
            return ret

        return f

    def __str__(self):
        return '<%s client %s:%d>' % (self.protocol.__name__, self.host, self.port)

    def __repr__(self):
        return str(self)

    def __nonzero__(self):
        return True

    def __hash__(self):
        return hash((self.host, self.port, self.protocol))

    def __eq__(self, rhs):
        return (isinstance(rhs, SimpleClient) and
                (self.host, self.port, self.protocol) == (rhs.host, rhs.port, rhs.protocol))

class MultiClient():
    """Abstract class representing a pool of servers."""
    def __init__(self, protocol, frame=False, timeout=None):
        self.protocol = protocol
        self.frame = frame
        self.timeout = None
        self.servers = []
        self.server_dict = {}

    def random_server(self):
        if not self.servers:
            return None
        return random.choice(self.servers)

    def get_server(self, name=None, host=None, port=None):
        if name:
            if name in self.server_dict:
                return self.server_dict[name]
            else:
                raise KeyError('No server named: %r' % name)
        elif host and port:
            host, port = _canonicalize_hostport(host, port)
            for server in self.servers:
                if (server.host, server.port) == (host, port):
                    return server
            else:
                raise KeyError('No server named: %r' % name)
        else:
            raise KeyError('No server identifier given.')

    def add_server(self, host=None, port=None, server=None, name=None):
        """Adds a server to the client pool. If server is not defined, then a new one
        with the given host and port is created as a SimpleClient. Returns the server
        that was just added."""
        if not server:
            server = SimpleClient(self.protocol, host, port, self.frame, None, self.timeout, name)
        self.servers.append(server)
        if name in self.server_dict:
            raise ValueError('Duplicate server name in server pool.')
        self.server_dict[server.name] = server
        return server

    def remove_server(self, server=None, name=None, host=None, port=None):
        """Removes the server from the pool, or removes the server with the indicated
        host and port from the pool."""
        if server:
            self.servers.remove(server)
            del self.server_dict[server.name]
        elif name:
            server = self.get_server(name=name)
            self.remove_server(server=server)
        else:
            host, port = _canonicalize_hostport(host, port)
            to_remove = [s for s in self.servers if (host, port) == (s.host, s.port)]
            for server in to_remove:
                self.servers.remove(server)
                del self.server_dict[server.name]

class ReplicatedClient(MultiClient):
    """Returns a new instance of the ReplicatedClient class. The ReplicatedClient represents
    a pool of servers all of whom expect to get every Thrift call called on the client. To
    add servers to the pool, call add_server(host, port) on the ReplicatedClient."""

    def __getattr__(self, k):
        """Proxies the request for the function with name 'k' to all of the servers added
        to the pool. The return result is a list of ThriftResponse objects, which contain
        the responses of each of the server, as well as the server that was hit. If there
        was an Exception raised by a server, a ThriftExceptionResponse object is returned
        instead."""
        def f(*args, **kwargs):
            responses = []
            for server in self.servers:
                try:
                    response = ThriftResponse(server, getattr(server, k)(*args, **kwargs))
                except Exception, e:
                    response = ThriftExceptionResponse(server, e)
                responses.append(response)
            return responses
        return f

    def __str__(self):
        return '<replicated %r>' % self.servers

    def __repr__(self):
        return str(self)

class ThreadedReplicatedClient(ReplicatedClient):
    """Returns a new instance of the ThreadedReplicatedClient class. A
    ThreadedReplicatedClient is identical to a ReplicatedClient, except that function calls
    are executed by threads instead of serially."""
    def __getattr__(self, k):
        def f(*args, **kwargs):
            responses = []
            def get_response(server):
                try:
                    response = ThriftResponse(server, getattr(server, k)(*args, **kwargs))
                except Exception, e:
                    response = ThriftExceptionResponse(server, e)
                # list.append() is thread-safe
                responses.append(response)

            threads = []
            for server in self.servers:
                threads.append(threading.Thread(target=get_response, args=(server,)))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            return responses

        return f

    def __str__(self):
        return '<threaded replicated %r>' % self.servers

class HashClient(MultiClient):
    """Returns a new instance of the HashClient class. A HashClient represents a pool of
    servers. When a Thrift call comes in, its parameters are hashed, and one of the given
    servers is used to respond to the Thrift request."""
    def __init__(self, protocol, frame=False, timeout=None):
        MultiClient.__init__(self, protocol, frame, timeout)
        self.all = ReplicatedClient(protocol, frame, timeout)
        self.hashfns = {}

    def add_server(self, host=None, port=None, server=None):
        """Adds a server to the client pool. If server is not defined, then a new one
        with the given host and port is created as a SimpleClient. Returns the server
        that was just added."""
        server = MultiClient.add_server(self, host, port, server)
        self.all.add_server(server=server)
        return server

    def remove_server(self, server=None, host=None, port=None):
        """Removes the server from the pool, or removes the server with the indicated
        host and port from the pool."""
        MultiClient.remove_server(self, server, host, port)
        self.all.remove_server(server, host, port)

    def set_hash(self, fnname, hashfn):
        """Changes the default hash function for the function named 'fnname' to hashfn.
        hashfn() expects the same parameters that the function defined as 'fnname'
        expects."""
        self.hashfns[fnname] = hashfn
        return self

    def __getattr__(self, k):
        """Proxy function for executing Thrift calls. When a call is made, the parameters
        of the function call are hashed, and a corresponding server is chosen to serve
        the request. If servers are added or removed, it is not guaranteed that future
        function calls with identical parameters will be sent to the same server. If an
        Exception is raised, the Exception returned will also contain a server property
        that represents the server object that served the request."""
        def f(*args, **kwargs):
            if k in self.hashfns:
                hashkey = self.hashfns[k](*args, **kwargs)
            else:
                hashkey = args + tuple(sorted(kwargs.items()))
            hashval = hash(hashkey)
            server_index = hashval % len(self.servers)
            server = self.servers[server_index]
            try:
                ret = getattr(server, k)(*args, **kwargs)
                response = ThriftResponse(server, ret)
            except Exception, e:
                e.server = server
                response = ThriftExceptionResponse(server, e)
            return response

        f.set_hash = lambda fn: self.set_hash(k, fn)
        return f

    def __str__(self):
        return '<hash client %r>' % self.servers

    def __repr__(self):
        return str(self)

class ThreadedHashClient(HashClient):
    """Returns a new instance of the ThreadedHashClient class. A ThreadedHashClient is
    identical to a HashClient, except that operations executed on 'all' are done in a
    threaded manner."""
    def __init__(self, protocol, frame=False, timeout=None):
        HashClient.__init__(self, protocol, frame, timeout)
        self.all = ThreadedReplicatedClient(protocol, frame, timeout)

    def __str__(self):
        return '<threaded hash client %r>' % self.servers
