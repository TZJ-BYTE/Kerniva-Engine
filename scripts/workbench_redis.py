"""Read-only RESP2 data adapter for the Redis and Kerniva workbench."""

import base64
import binascii
from contextlib import ExitStack
from dataclasses import dataclass
import hashlib
import json
import math
import socket
import ssl
import time


_MAX_REPLY_BYTES = 32 * 1024 * 1024
_MAX_REPLY_ITEMS = 32768
_MAX_KEY_BYTES = 1024 * 1024
_MAX_PAGE_COUNT = 200
_MAX_PREVIEW_BYTES = 65536
_MAX_UINT64 = (1 << 64) - 1
_READ_COMMANDS = {
    "scan": ("SCAN",),
    "string": ("STRLEN", "GETRANGE"),
    "hash": ("HSCAN",),
    "list": ("LRANGE",),
    "set": ("SSCAN",),
    "zset": ("ZRANGE",),
    "stream": ("XRANGE",),
}


class RedisWorkbenchError(Exception):
    """A Redis connection, protocol or operation error."""


class _CommandError(RedisWorkbenchError):
    pass


@dataclass
class _BulkPreview:
    data: bytes
    truncated: bool


def _b64(data):
    return base64.b64encode(data).decode("ascii")


def _text(data):
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _key_item(data):
    if not isinstance(data, bytes) or len(data) > _MAX_KEY_BYTES:
        raise RedisWorkbenchError("Key exceeds the workbench key size limit")
    return {"key": _text(data), "key_b64": _b64(data)}


def _value(data, *, more=False):
    if isinstance(data, _BulkPreview):
        return _value(data.data, more=more or data.truncated)
    if not isinstance(data, bytes):
        raise RedisWorkbenchError("Unexpected Redis value")
    return {"text": _text(data), "b64": _b64(data), "truncated": more}


def _number(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer between {low} and {high}")
    return value


def _option(profile, key, default=""):
    value = profile.get(key, default)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _argument(value):
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


class _Connection:
    def __init__(self, owner, address):
        self.owner = owner
        self.address = address

    def __enter__(self):
        self.resources = ExitStack()
        try:
            host = self.address[0]
            raw = self.resources.enter_context(socket.create_connection(self.address, self.owner.remaining()))
            raw.settimeout(self.owner.remaining())
            self.socket = raw
            if self.owner.tls:
                self.socket = self.resources.enter_context(
                    self.owner.tls.wrap_socket(raw, server_hostname=host)
                )
            self.buffer = bytearray()
            username, password = self.owner.username, self.owner.password
            if username:
                self.command("AUTH", username, password)
            elif password:
                self.command("AUTH", password)
            if self.owner.db:
                self.command("SELECT", self.owner.db)
            return self
        except BaseException:
            self.resources.close()
            raise

    def __exit__(self, *args):
        self.resources.close()

    def command(self, *args, preview=None):
        arguments = [_argument(arg) for arg in args]
        frame = [f"*{len(arguments)}\r\n".encode("ascii")]
        for arg in arguments:
            frame.extend((f"${len(arg)}\r\n".encode("ascii"), arg, b"\r\n"))
        self.socket.settimeout(self.owner.remaining())
        self.socket.sendall(b"".join(frame))
        self.bytes_left = _MAX_REPLY_BYTES
        self.items_left = _MAX_REPLY_ITEMS
        return self._reply(preview)

    def _receive(self, limit):
        self.socket.settimeout(self.owner.remaining())
        data = self.socket.recv(min(limit, 65536))
        if not data:
            raise RedisWorkbenchError("Incomplete Redis response")
        return data

    def _read(self, length):
        if length > self.bytes_left:
            raise RedisWorkbenchError("Redis response exceeds the workbench size limit")
        data = bytearray(self.buffer[:length])
        del self.buffer[:length]
        while len(data) < length:
            data.extend(self._receive(length - len(data)))
        self.bytes_left -= length
        return bytes(data)

    def _line(self):
        limit = min(4096, self.bytes_left)
        while True:
            end = self.buffer.find(b"\r\n")
            if 0 <= end <= limit - 2:
                line = bytes(self.buffer[:end])
                del self.buffer[:end + 2]
                self.bytes_left -= end + 2
                return line
            if len(self.buffer) >= limit:
                raise RedisWorkbenchError("Invalid Redis response header")
            self.buffer.extend(self._receive(limit - len(self.buffer)))

    def _reply(self, preview, depth=0):
        self.items_left -= 1
        if self.items_left < 0 or depth > 16:
            raise RedisWorkbenchError("Redis response exceeds the workbench nesting or item limit")
        kind, line = self._read(1), self._line()
        if kind == b"+":
            return line
        if kind == b"-":
            raise _CommandError(line.decode("utf-8", errors="replace"))
        try:
            number = int(line)
        except ValueError as exc:
            raise RedisWorkbenchError("Invalid Redis response") from exc
        if kind == b":":
            return number
        if number == -1 and kind in (b"$", b"*"):
            return None
        if number < 0:
            raise RedisWorkbenchError("Invalid Redis response length")
        if kind == b"$":
            if number + 2 > self.bytes_left:
                raise RedisWorkbenchError("Redis response exceeds the workbench size limit")
            keep = number if preview is None else min(number, preview)
            data = self._read(keep)
            discard = number - keep
            while discard:
                block = min(discard, 65536)
                self._read(block)
                discard -= block
            if self._read(2) != b"\r\n":
                raise RedisWorkbenchError("Invalid Redis bulk string")
            return _BulkPreview(data, True) if keep < number else data
        if kind == b"*" and number <= self.items_left:
            return [self._reply(preview, depth + 1) for _ in range(number)]
        raise RedisWorkbenchError("Invalid or oversized Redis response")


def _address(host, port):
    if not isinstance(host, str) or not host or any(ch.isspace() or ch == "\x00" for ch in host):
        raise ValueError("host must be a hostname or IP address")
    host = host.strip("[]")
    if not host:
        raise ValueError("host must be a hostname or IP address")
    return host, _number(port, "port", 1, 65535)


def _address_label(address):
    host, port = address
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _cluster_slot(key):
    start = key.find(b"{")
    if start >= 0:
        end = key.find(b"}", start + 1)
        if end > start + 1:
            key = key[start + 1:end]
    return binascii.crc_hqx(key, 0) % 16384


class _Client:
    def __init__(self, profile):
        self.seed = _address(profile.get("host", "127.0.0.1"), profile.get("port", 6379))
        self.mode = profile.get("mode", "standalone")
        if self.mode not in ("standalone", "cluster"):
            raise ValueError("mode must be standalone or cluster")
        self.db = _number(profile.get("db", 0), "db", 0, 2**31 - 1)
        if self.mode == "cluster" and self.db:
            raise ValueError("Redis Cluster only supports database 0")
        self.username = _option(profile, "username")
        self.password = _option(profile, "password")
        timeout = profile.get("timeout", 3)
        if type(timeout) not in (float, int) or not 0 < timeout <= 60 or not math.isfinite(timeout):
            raise ValueError("timeout must be between 0 and 60 seconds")
        use_tls = profile.get("tls", False)
        if not isinstance(use_tls, bool):
            raise ValueError("tls must be a boolean")
        self.tls = None
        if use_tls:
            try:
                self.tls = ssl.create_default_context(cafile=_option(profile, "caFile") or None)
            except (OSError, ssl.SSLError) as exc:
                raise RedisWorkbenchError(f"Could not load TLS trust configuration: {exc}") from exc
        self.deadline = time.monotonic() + timeout
        self.connections = {}
        self.resources = ExitStack()
        self.slots = {}
        self.nodes = {}
        self.scope = hashlib.sha256(repr((self.seed, self.mode, self.db, self.username, use_tls)).encode()).hexdigest()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.resources.close()

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("Redis operation timed out")
        return left

    def at(self, address, *args, asking=False, preview=None):
        try:
            conn = self.connections.get(address)
            if conn is None:
                conn = self.resources.enter_context(_Connection(self, address))
                self.connections[address] = conn
            if asking:
                conn.command("ASKING")
            return conn.command(*args, preview=preview)
        except OSError as exc:
            raise RedisWorkbenchError(f"Redis connection to {_address_label(address)} failed: {exc}") from exc

    def discover(self):
        if self.mode == "standalone":
            self.nodes = {_address_label(self.seed): self.seed}
            return
        slots = _array(self.at(self.seed, "CLUSTER", "SLOTS"))
        if not slots:
            raise RedisWorkbenchError("Redis Cluster returned no primary nodes")
        new_slots = {}
        nodes = {}
        for row in slots:
            row = _array(row)
            if len(row) < 3:
                raise RedisWorkbenchError("Invalid Redis Cluster slot range")
            start, end, primary = row[:3]
            primary = _array(primary)
            if len(primary) < 2 or type(start) is not int or type(end) is not int or not 0 <= start <= end < 16384:
                raise RedisWorkbenchError("Invalid Redis Cluster slot range")
            endpoint, port = primary[:2]
            metadata = primary[3] if len(primary) > 3 and isinstance(primary[3], list) else []
            addresses = dict(zip(metadata[::2], metadata[1::2]))
            preferred = addresses.get(b"hostname") if self.tls else None
            if endpoint == b"?":
                endpoint = addresses.get(b"hostname") or addresses.get(b"ip") or b"?"
            if endpoint == b"?":
                raise RedisWorkbenchError("Redis Cluster advertises an unknown primary address")
            try:
                host = (preferred or endpoint).decode("utf-8") if preferred or endpoint else self.seed[0]
                address = _address(host, port)
            except (AttributeError, ValueError) as exc:
                raise RedisWorkbenchError("Invalid Redis Cluster primary address") from exc
            node_id = primary[2] if len(primary) > 2 else None
            node_id = _text(node_id) if isinstance(node_id, bytes) else None
            nodes[node_id or _address_label(address)] = address
            for slot in range(start, end + 1):
                new_slots[slot] = address
        self.nodes, self.slots = dict(sorted(nodes.items())), new_slots

    def keyed(self, command, key, *args, preview=None):
        if self.mode == "standalone":
            return self.at(self.seed, command, key, *args, preview=preview)
        slot = _cluster_slot(key)
        address = self.slots.get(slot, self.seed)
        asking = False
        for _ in range(6):
            try:
                return self.at(address, command, key, *args, asking=asking, preview=preview)
            except _CommandError as exc:
                parts = str(exc).split()
                if len(parts) != 3 or parts[0] not in ("MOVED", "ASK"):
                    raise
                try:
                    host, port = parts[2].rsplit(":", 1)
                    new_slot = int(parts[1])
                    if new_slot != slot:
                        raise ValueError("redirection slot mismatch")
                    address = _address(host or address[0], int(port))
                except ValueError as invalid:
                    raise RedisWorkbenchError("Invalid Redis Cluster redirection") from invalid
                asking = parts[0] == "ASK"
                if not asking:
                    self.slots[slot] = address
        raise RedisWorkbenchError("Redis Cluster redirected the operation too many times")


def _array(value):
    if not isinstance(value, list):
        raise RedisWorkbenchError("Unexpected Redis array response")
    return value


def _required_bytes(value):
    if isinstance(value, _BulkPreview):
        raise RedisWorkbenchError("Redis response metadata exceeds the preview limit")
    if not isinstance(value, bytes):
        raise RedisWorkbenchError("Unexpected Redis text response")
    return value


def _cursor_pack(scope, *positions):
    payload = json.dumps([scope, *positions], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _cursor_unpack(token, scope, length):
    try:
        if not isinstance(token, str) or len(token) > 4096:
            raise ValueError
        values = json.loads(base64.b64decode(token, altchars=b"-_", validate=True))
        if not isinstance(values, list) or len(values) != length + 1 or values[0] != scope:
            raise ValueError
        return values[1:]
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise ValueError("Cursor is invalid or belongs to a different request") from exc


def _scope(client, kind, *values):
    return hashlib.sha256(repr((client.scope, kind, *values)).encode()).hexdigest()


def _scan_response(response):
    fields = _array(response)
    if len(fields) != 2:
        raise RedisWorkbenchError("Invalid Redis SCAN response")
    try:
        next_cursor = int(_required_bytes(fields[0]))
    except ValueError as exc:
        raise RedisWorkbenchError("Invalid Redis SCAN cursor") from exc
    if not 0 <= next_cursor <= _MAX_UINT64:
        raise RedisWorkbenchError("Invalid Redis SCAN cursor")
    return next_cursor, _array(fields[1])


def _scan_page(read, state, count, stride=1):
    position, offset, previous_count = state
    _number(position, "cursor position", 0, _MAX_UINT64)
    _number(offset, "cursor offset", 0, _MAX_REPLY_ITEMS)
    _number(previous_count, "cursor count", 0, _MAX_PAGE_COUNT)
    batch_count = previous_count or count
    next_pos, records = _scan_response(read(position, batch_count))
    if len(records) % stride:
        raise RedisWorkbenchError("Invalid Redis scan record")
    start = offset * stride
    stop = start + count * stride
    if stop < len(records):
        next_state = (position, offset + count, batch_count)
    else:
        next_state = (next_pos, 0, 0) if next_pos else None
    return records[start:stop], next_state


def _unsupported(exc):
    detail = str(exc).lower()
    return (
        detail.startswith("noperm")
        or "unknown command" in detail
        or "not supported" in detail
        or "unsupported" in detail
    )


def _info_fields(client, address):
    payload = _required_bytes(client.at(address, "INFO"))
    fields = {}
    for line in payload.decode("utf-8", errors="replace").splitlines():
        name, sep, value = line.partition(":")
        if sep:
            fields[name] = value
    return fields


def _server_name(info):
    if "storage_backend" in info or "go_version" in info:
        return "Kerniva"
    return ("Redis " + info.get("redis_version", "")).rstrip()


def _info_number(info, key):
    try:
        return int(info[key])
    except (KeyError, TypeError, ValueError):
        return None


def status(profile):
    """Only report counters exposed by this node. Redis DB and instance counters have different scopes."""
    with _Client(profile) as client:
        client.discover()
        nodes = []
        for node_id, address in client.nodes.items():
            node = {"id": node_id, "label": _address_label(address), "host": address[0], "port": address[1]}
            nodes.append(node)
            try:
                info = _info_fields(client, address)
            except RedisWorkbenchError as error:
                node["error"] = str(error).replace(client.password, "••••") if client.password else str(error)
                continue
            kerniva = "go_version" in info or "storage_backend" in info
            node.update(server=_server_name(info), version=info.get("redis_version"),
                        role=info.get("role"), runtime=info.get("go_version"), storageBackend=info.get("storage_backend"),
                        durability=info.get("persistence_durability"), heapBytes=_info_number(info, "heap_alloc_bytes"),
                        gcCount=_info_number(info, "num_gc"),
                        uptimeSeconds=_info_number(info, "uptime_in_seconds"),
                        usedMemoryBytes=_info_number(info, "used_memory_bytes" if kerniva else "used_memory"),
                        usedMemoryScope="db" if kerniva else "instance",
                        connectedClients=_info_number(info, "connected_clients"),
                        opsPerSecond=_info_number(info, "instantaneous_ops_per_sec"),
                        sequence=_info_number(info, "last_sequence"), keys=None)
            count = info.get(f"db{client.db}", "").split(",")
            for part in count:
                if part.startswith("keys="):
                    node["keys"] = _info_number({"keys": part[5:]}, "keys")
            if node["keys"] is None:
                try:
                    size = client.at(address, "DBSIZE")
                    node["keys"] = size if type(size) is int and size >= 0 else None
                except RedisWorkbenchError as error:
                    node["keysError"] = str(error).replace(client.password, "••••") if client.password else str(error)
        return {"mode": client.mode, "db": client.db, "nodes": nodes,
                "server": next((node["server"] for node in nodes if node.get("server")), None)}


def probe(profile):
    """Check the connection and describe supported read operations."""
    with _Client(profile) as client:
        client.at(client.seed, "PING")
        client.discover()
        info = {}
        try:
            info = _info_fields(client, client.seed)
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
        server = _server_name(info)
        capabilities = dict.fromkeys(_READ_COMMANDS, True)
        commands = list(dict.fromkeys(cmd for group in _READ_COMMANDS.values() for cmd in group))
        try:
            result = _array(client.at(client.seed, "COMMAND", "INFO", *commands))
            if len(result) == len(commands):
                supported = dict(zip(commands, (entry is not None for entry in result)))
                capabilities = {
                    name: all(supported[cmd] for cmd in group) for name, group in _READ_COMMANDS.items()
                }
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
        nodes = [
            {"id": node, "label": _address_label(address), "host": address[0], "port": address[1]}
            for node, address in client.nodes.items()
        ]
        result = {"ok": True, "mode": client.mode, "server": server, "nodes": nodes,
                  "capabilities": capabilities}
        if info.get("storage_backend"):
            result["storage_backend"] = info["storage_backend"]
        return result


def scan(profile, request):
    """Scan one page from a standalone server or a cluster primary."""
    count = _number(request.get("count", 50), "count", 1, _MAX_PAGE_COUNT)
    pattern = request.get("pattern", "*")
    if not isinstance(pattern, str) or len(pattern) > 4096:
        raise ValueError("pattern must be a string of up to 4096 characters")
    selected = request.get("node")
    if selected is not None and not isinstance(selected, str):
        raise ValueError("node must be a node ID")
    with _Client(profile) as client:
        client.discover()
        nodes = list(client.nodes)
        if selected:
            if selected not in client.nodes:
                raise ValueError("Unknown scan node")
            nodes = [selected]
        scope = _scope(client, "scan", pattern, tuple((node, client.nodes[node]) for node in nodes))
        token = request.get("cursor")
        index, *state = _cursor_unpack(token, scope, 4) if token else (0, 0, 0, 0)
        _number(index, "cursor node", 0, len(nodes) - 1)
        node = nodes[index]

        def read(position, batch_count):
            return client.at(client.nodes[node], "SCAN", position, "MATCH", pattern, "COUNT", batch_count)

        try:
            keys, next_state = _scan_page(read, state, count)
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
            return {"items": [], "cursor": None, "done": True, "unsupported": str(exc)}
        if next_state is None:
            index += 1
            next_state = (0, 0, 0)
        cursor = _cursor_pack(scope, index, *next_state) if index < len(nodes) else None
        return {"items": [_key_item(key) for key in keys], "cursor": cursor,
                "done": cursor is None, "node": node}


def _pairs(values):
    values = _array(values)
    if len(values) % 2:
        raise RedisWorkbenchError("Invalid Redis field/value response")
    return zip(values[::2], values[1::2])


def _read_key(request):
    encoded = request.get("key_b64")
    if encoded is not None:
        if not isinstance(encoded, str) or len(encoded) > 4 * ((_MAX_KEY_BYTES + 2) // 3):
            raise ValueError("Invalid base64 key")
        try:
            key = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("Invalid base64 key") from exc
    else:
        key = request.get("key")
        if not isinstance(key, str):
            raise ValueError("key or key_b64 is required")
        key = key.encode("utf-8")
    if len(key) > _MAX_KEY_BYTES:
        raise ValueError("Key exceeds the workbench key size limit")
    return key


def _next_stream_id(raw):
    try:
        milliseconds, sequence = map(int, _required_bytes(raw).split(b"-"))
        if not 0 <= milliseconds <= _MAX_UINT64 or not 0 <= sequence <= _MAX_UINT64:
            raise ValueError
    except ValueError as exc:
        raise RedisWorkbenchError("Invalid Redis stream entry ID") from exc
    if sequence < _MAX_UINT64:
        return f"{milliseconds}-{sequence + 1}"
    if milliseconds < _MAX_UINT64:
        return f"{milliseconds + 1}-0"
    return None


def _inspect(client, key, kind, state, count, preview, result):
    position = state[0]
    # Cursor and stream IDs can be longer than a small value preview.
    wire_preview = max(128, preview)

    def read(command, *args):
        return client.keyed(command, key, *args, preview=wire_preview)

    def value(data):
        if isinstance(data, _BulkPreview):
            return _value(data.data[:preview], more=True)
        if not isinstance(data, bytes):
            raise RedisWorkbenchError("Unexpected Redis value")
        return _value(data[:preview], more=len(data) > preview)

    if kind == "stream":
        if position != 0:
            if not isinstance(position, str) or len(position) > 41:
                raise ValueError("Invalid stream cursor")
            _next_stream_id(position.encode("ascii"))
        items = _array(read("XRANGE", position if position else "-", "+", "COUNT", count + 1))
        for row in items[:count]:
            row = _array(row)
            if len(row) != 2:
                raise RedisWorkbenchError("Invalid Redis stream entry")
            entry_id, fields = row
            result["items"].append({
                "id": _required_bytes(entry_id).decode("ascii"),
                "fields": [{"field": value(field), "value": value(field_value)}
                           for field, field_value in _pairs(fields)],
            })
        next_id = _next_stream_id(items[count - 1][0]) if len(items) > count else None
        return (next_id, 0, 0) if next_id is not None else None

    _number(position, "cursor position", 0, _MAX_UINT64)
    if kind == "string":
        size = read("STRLEN")
        if type(size) is not int or size < 0:
            raise RedisWorkbenchError("Invalid Redis string length")
        data = b"" if position >= size else read("GETRANGE", position, min(size, position + preview) - 1)
        data = _required_bytes(data)
        next_pos = position + len(data)
        more = bool(data) and next_pos < size
        result.update(value=_value(data, more=more), length=size, offset=position)
        return (next_pos, 0, 0) if more else None
    if kind == "hash":
        entries, next_state = _scan_page(
            lambda pos, batch: read("HSCAN", pos, "COUNT", batch), state, count, stride=2
        )
        result["items"] = [{"field": value(field), "value": value(field_value)}
                           for field, field_value in _pairs(entries)]
        return next_state
    if kind == "set":
        entries, next_state = _scan_page(
            lambda pos, batch: read("SSCAN", pos, "COUNT", batch), state, count
        )
        result["items"] = [{"value": value(entry)} for entry in entries]
        return next_state
    if kind == "list":
        items = _array(read("LRANGE", position, position + count))
        result["items"] = [{"value": value(entry)} for entry in items[:count]]
        return (position + count, 0, 0) if len(items) > count else None
    if kind == "zset":
        items = list(_pairs(read("ZRANGE", position, position + count, "WITHSCORES")))
        result["items"] = [{"member": value(member), "score": _required_bytes(score).decode("ascii")}
                           for member, score in items[:count]]
        return (position + count, 0, 0) if len(items) > count else None
    result["unsupported"] = f"Key type {kind} has no preview"
    return None


def inspect_key(profile, request):
    """Inspect one typed, bounded page for a binary-safe Redis key."""
    key = _read_key(request)
    count = _number(request.get("count", 50), "count", 1, _MAX_PAGE_COUNT)
    preview = _number(request.get("previewBytes", 512), "previewBytes", 1, _MAX_PREVIEW_BYTES)
    with _Client(profile) as client:
        client.discover()
        result = {**_key_item(key), "type": None, "ttl_ms": None, "items": [], "done": True, "cursor": None}
        try:
            kind = _required_bytes(client.keyed("TYPE", key)).decode("ascii")
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
            return {**result, "unsupported": str(exc)}
        result["type"] = kind
        try:
            ttl = client.keyed("PTTL", key)
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
            try:
                ttl = client.keyed("TTL", key)
                if type(ttl) is int and ttl >= 0:
                    ttl *= 1000
            except _CommandError as fallback:
                if not _unsupported(fallback):
                    raise
                ttl = None
        if ttl is not None and type(ttl) is not int:
            raise RedisWorkbenchError("Invalid Redis TTL response")
        if kind == "none" or ttl == -2:
            return {**result, "type": "none", "ttl_ms": -2}
        result["ttl_ms"] = ttl
        scope = _scope(client, "inspect", key, kind)
        token = request.get("cursor")
        state = _cursor_unpack(token, scope, 3) if token else (0, 0, 0)
        try:
            next_state = _inspect(client, key, kind, state, count, preview, result)
        except _CommandError as exc:
            if not _unsupported(exc):
                raise
            result["unsupported"] = str(exc)
            return result
        if next_state is not None:
            result.update(cursor=_cursor_pack(scope, *next_state), done=False)
        return result
