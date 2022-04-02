import json
import socket
import threading

from .console_display_service import ConsoleDisplayService
from .ipython_display_service import IPythonDisplayService
from ...common.utils.packages import in_ipython
# noinspection PyProtectedMember
from ...config import g_config


class DataStreamCollectorThread(threading.Thread):

    @staticmethod
    def get_display_service_cls():
        if in_ipython():
            return IPythonDisplayService
        else:
            return ConsoleDisplayService

    def __init__(self, op, key, refresh_interval, max_limit, port):
        super(DataStreamCollectorThread, self).__init__()
        self.op = op
        self.key = key
        self.max_limit = max_limit
        self.stop_event = threading.Event()
        self.counter = 0
        self.refresh_interval = refresh_interval
        self.lock = threading.Lock()

        # create server socket
        self.server_socket = socket.socket()
        self.server_socket.bind(('', port))
        self.port = self.server_socket.getsockname()[1]

        # append socket writer
        local_ip = g_config["local_ip"]
        from ..common import SocketSinkStreamOp
        self.op.link(SocketSinkStreamOp().setHost(local_ip).setPort(self.port))

        # create display service
        display_service_cls = DataStreamCollectorThread.get_display_service_cls()
        self.display_service = display_service_cls(self.op, self.key, self.refresh_interval, self.max_limit)

    def stop(self):
        with self.lock:
            self.stop_event.set()

    def recvall(self, conn, bufsize):
        data = b''
        while not self.stop_event.is_set():
            buf = conn.recv(bufsize)
            data += buf
            if len(data) > 0:
                try:
                    # if current `data` can be decoded, return decoded string
                    s = data.decode("utf-8")
                    return s
                except UnicodeError:
                    # if the received data cannot be decoded using utf-8, more data is needed
                    pass
            # connection is closed
            if len(buf) == 0:
                return None

    def run(self):
        self.server_socket.listen()
        conn, _ = self.server_socket.accept()

        # store incomplete message
        content = ''
        counter = 0
        while not self.stop_event.is_set():
            with self.lock:
                if not self.stop_event.is_set():
                    new_content = self.recvall(conn, 4096)
            if new_content is None:
                break
            content += new_content
            items, content = self.extract_data(content)
            counter += len(items)
            with self.lock:
                if not self.stop_event.is_set():
                    self.display_service.accept_items(items)

        conn.close()
        self.display_service.stop()
        self.server_socket.close()

    @staticmethod
    def skip_data(content: str):
        return content.rsplit("\r\n", maxsplit=1)[-1]

    @staticmethod
    def extract_data(content):
        splits = content.split("\r\n")
        items = []
        for index, line in enumerate(splits):
            if line == '':
                continue
            try:
                obj = json.loads(line)
                fields = obj['fields']
                items.append(fields)
            except:
                return items, line
        return items, ''
