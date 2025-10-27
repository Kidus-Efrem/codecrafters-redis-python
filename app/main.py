import asyncio
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any
import traceback

class DataHandler:
    _instance = None

    def __init__(self):
        self.strings: Dict[str, Tuple[str, float]] = {}
        self.lists: Dict[str, List[str]] = defaultdict(list)
        self.streams: Dict[str, Dict[str, List[List[str]]]] = defaultdict(dict)
        self.last_used_seq: Dict[int, int] = {}
        self.last_used_time = 0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def handle_set(self, key: str, value: str, opts: dict = None) -> None:
        expiry = float('inf')
        if opts and 'px' in opts:
            expiry = time.time() + int(opts['px']) / 1000
        self.strings[key] = (value, expiry)

    def handle_get(self, key: str) -> Optional[str]:
        if key in self.strings:
            value, expiry = self.strings[key]
            if time.time() < expiry:
                return value
            else:
                del self.strings[key]
        return None

    def handle_list_set(self, key: str, values: List[str], append: bool = True) -> int:
        if append:
            self.lists[key].extend(values)
        else:
            for v in reversed(values):
                self.lists[key].insert(0, v)
        return len(self.lists[key])

    def handle_lpop(self, key: str, count: int = 1) -> Optional[List[str]]:
        if key not in self.lists or not self.lists[key]:
            return None

        if count == 1:
            return [self.lists[key].pop(0)]
        else:
            popped = []
            for _ in range(min(count, len(self.lists[key]))):
                popped.append(self.lists[key].pop(0))
            return popped

    def handle_lrange(self, key: str, start: int, end: int) -> List[str]:
        if key not in self.lists:
            return []

        arr = self.lists[key]
        n = len(arr)

        if start < 0:
            start += n
        if end < 0:
            end += n

        start = max(0, start)
        end = min(end, n - 1)

        if start > end:
            return []

        return arr[start:end + 1]

    def handle_llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    def handle_xadd(self, key: str, stream_id: str, fields: List[str]) -> str:
        # Handle auto-generated ID
        if stream_id == '*':
            current_time = int(time.time() * 1000)
            if current_time in self.last_used_seq:
                sequence = self.last_used_seq[current_time] + 1
            else:
                sequence = 0
            stream_id = f"{current_time}-{sequence}"
        else:
            # Parse manual ID
            parts = stream_id.split('-')
            if len(parts) == 2:
                ts_part, seq_part = parts
                current_time = int(ts_part) if ts_part else int(time.time() * 1000)
                if seq_part == '*':
                    if current_time in self.last_used_seq:
                        sequence = self.last_used_seq[current_time] + 1
                    else:
                        sequence = 0
                else:
                    sequence = int(seq_part)
            else:
                current_time = int(stream_id)
                sequence = 0

        # Validate ID
        if current_time == 0 and sequence == 0:
            raise Exception("The ID specified in XADD must be greater than 0-0")

        if current_time < self.last_used_time:
            raise Exception("The ID specified in XADD is equal or smaller than the target stream top item")

        if current_time == self.last_used_time and sequence <= self.last_used_seq.get(current_time, -1):
            raise Exception("The ID specified in XADD is equal or smaller than the target stream top item")

        # Update sequence tracking
        self.last_used_time = current_time
        self.last_used_seq[current_time] = sequence

        final_id = f"{current_time}-{sequence}"

        # Store the fields as pairs
        field_pairs = []
        for i in range(0, len(fields), 2):
            if i + 1 < len(fields):
                field_pairs.append([fields[i], fields[i + 1]])

        self.streams[key][final_id] = field_pairs
        return final_id

    def get_last_stream_id(self, key: str) -> str:
        """Get the last ID in a stream, returns '0-0' if stream doesn't exist"""
        if not self.streams[key]:
            return '0-0'
        return max(self.streams[key].keys(), key=lambda x: self._parse_stream_id(x))

    def handle_xread(self, keys: List[str], ids: List[str], block: bool = False) -> List[Tuple[str, List]]:
        """Handle XREAD command - returns list of (key, entries)"""
        results = []

        for key, start_id in zip(keys, ids):
            if key not in self.streams:
                results.append((key, []))
                continue

            # Handle $ special case - only return entries added after current last ID
            if start_id == '$':
                # For non-blocking XREAD with $, return empty immediately
                if not block:
                    results.append((key, []))
                    continue
                # For blocking XREAD with $, we'll handle this in the blocking logic
                # by using the current last ID as the start_id
                start_id = self.get_last_stream_id(key)

            entries = []
            for entry_id, field_pairs in self.streams[key].items():
                if self._compare_stream_ids(entry_id, start_id) > 0:
                    entries.append((entry_id, field_pairs))

            # Sort by ID
            entries.sort(key=lambda x: self._parse_stream_id(x[0]))
            results.append((key, entries))

        return results

    def handle_xrange(self, key: str, start_id: str, end_id: str) -> List:
        if key not in self.streams:
            return []

        entries = []
        for entry_id, field_pairs in self.streams[key].items():
            if (self._compare_stream_ids(entry_id, start_id) >= 0 and
                self._compare_stream_ids(entry_id, end_id) <= 0):
                entries.append((entry_id, field_pairs))

        # Sort by ID
        entries.sort(key=lambda x: self._parse_stream_id(x[0]))
        return entries

    def _parse_stream_id(self, stream_id: str) -> Tuple[int, int]:
        """Parse stream ID into (timestamp, sequence) tuple for comparison"""
        if '-' in stream_id:
            ts, seq = stream_id.split('-')
            return (int(ts), int(seq))
        return (int(stream_id), 0)

    def _compare_stream_ids(self, id1: str, id2: str) -> int:
        """Compare two stream IDs. Returns -1, 0, 1 for less, equal, greater"""
        ts1, seq1 = self._parse_stream_id(id1)
        ts2, seq2 = self._parse_stream_id(id2)

        if ts1 < ts2:
            return -1
        elif ts1 > ts2:
            return 1
        else:
            if seq1 < seq2:
                return -1
            elif seq1 > seq2:
                return 1
            else:
                return 0

class RESPBuilder:
    @property
    def SIMPLE_STR(self) -> bytes:
        return b"+OK\r\n"

    @property
    def PONG(self) -> bytes:
        return b"+PONG\r\n"

    @property
    def NULL_BULK_STR(self) -> bytes:
        return b"$-1\r\n"

    @property
    def NULL_ARRAY(self) -> bytes:
        return b"*-1\r\n"

    @property
    def EMPTY_RESP_ARRAY(self) -> bytes:
        return b"*0\r\n"

    @property
    def NONE(self) -> bytes:
        return b"+none\r\n"

    def bulk_str(self, val: str) -> bytes:
        return f"${len(val)}\r\n{val}\r\n".encode()

    def integer(self, val: int) -> bytes:
        return f":{val}\r\n".encode()

    def error(self, msg: str) -> bytes:
        return f"-ERR {msg}\r\n".encode()

    def resp_array(self, elements: List) -> bytes:
        if elements is None:
            return self.NULL_ARRAY

        result = f"*{len(elements)}\r\n".encode()

        for element in elements:
            if isinstance(element, str):
                result += self.bulk_str(element)
            elif isinstance(element, int):
                result += self.integer(element)
            elif isinstance(element, list):
                result += self.resp_array(element)
            elif isinstance(element, tuple):
                # Handle stream response format: (key, [(id, field_pairs), ...])
                key, entries = element
                result += self.resp_array([key, self._format_stream_entries(entries)])
        return result

    def _format_stream_entries(self, entries: List) -> bytes:
        """Format stream entries for RESP response"""
        result = f"*{len(entries)}\r\n".encode()
        for entry_id, field_pairs in entries:
            # Each entry is [id, [field1, value1, field2, value2, ...]]
            flattened_fields = []
            for pair in field_pairs:
                flattened_fields.extend(pair)
            result += self.resp_array([entry_id, self.resp_array(flattened_fields)])
        return result

class RESPParser:
    def parse(self, data: bytes) -> Tuple[List[Dict], int]:
        commands = []
        i = 0

        while i < len(data):
            if data[i] == ord('*'):  # Array
                i += 1
                j = data.find(b'\r\n', i)
                if j == -1:
                    break

                arr_len = int(data[i:j])
                i = j + 2

                elements = []
                for _ in range(arr_len):
                    if i >= len(data):
                        break

                    if data[i] == ord('$'):  # Bulk string
                        i += 1
                        j = data.find(b'\r\n', i)
                        if j == -1:
                            break
                        str_len = int(data[i:j])
                        i = j + 2

                        if i + str_len > len(data):
                            break

                        element = data[i:i + str_len].decode()
                        i += str_len + 2
                        elements.append(element)

                if elements:
                    cmd = self._parse_command(elements)
                    commands.append(cmd)

        return commands, i

    def _parse_command(self, elements: List[str]) -> Dict:
        cmd = {
            "cmd": elements[0].upper(),
            "args": elements[1:],
            "opts": {}
        }

        # Parse options for SET command
        if cmd["cmd"] == "SET" and len(elements) >= 4:
            for i in range(2, len(elements)-1):
                if elements[i].upper() == "PX":
                    cmd["opts"]["px"] = elements[i+1]

        return cmd

class AsyncRequestHandler:
    def __init__(self, server):
        self.server = server
        self.data_handler = DataHandler.get_instance()
        self.parser = RESPParser()
        self.builder = RESPBuilder()

    async def handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, data: bytes):
        try:
            commands, _ = self.parser.parse(data)

            for req in commands:
                cmd = req['cmd']
                args = req['args']
                opts = req['opts']

                print(f"{cmd} WITH {args}")

                if cmd == "PING":
                    await self.handle_ping(writer)
                elif cmd == "ECHO":
                    await self.handle_echo(writer, args)
                elif cmd == "SET":
                    await self.handle_set(writer, args, opts)
                elif cmd == "GET":
                    await self.handle_get(writer, args)
                elif cmd == "RPUSH":
                    await self.handle_rpush(writer, args)
                elif cmd == "LPUSH":
                    await self.handle_lpush(writer, args)
                elif cmd == "LLEN":
                    await self.handle_llen(writer, args)
                elif cmd == "LPOP":
                    await self.handle_lpop(writer, args)
                elif cmd == "LRANGE":
                    await self.handle_lrange(writer, args)
                elif cmd == "BLPOP":
                    await self.handle_blpop(writer, args)
                elif cmd == "TYPE":
                    await self.handle_type(writer, args)
                elif cmd == "XADD":
                    await self.handle_xadd(writer, args)
                elif cmd == "XRANGE":
                    await self.handle_xrange(writer, args)
                elif cmd == "XREAD":
                    await self.handle_xread(writer, args)
                elif cmd == "COMMAND":
                    await self.handle_ping(writer)
                else:
                    writer.write(self.builder.error(f"Unknown command {cmd}"))

                await writer.drain()

        except Exception as e:
            print(traceback.format_exc())
            writer.write(self.builder.error(str(e)))
            await writer.drain()

    async def handle_ping(self, writer):
        writer.write(self.builder.PONG)

    async def handle_echo(self, writer, args):
        writer.write(self.builder.bulk_str(args[0]))

    async def handle_set(self, writer, args, opts):
        try:
            key, val = args[0], args[1]
            self.data_handler.handle_set(key, val, opts)
            writer.write(self.builder.SIMPLE_STR)
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_get(self, writer, args):
        try:
            key = args[0]
            val = self.data_handler.handle_get(key)
            if val is None:
                writer.write(self.builder.NULL_BULK_STR)
            else:
                writer.write(self.builder.bulk_str(val))
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_rpush(self, writer, args):
        try:
            key, vals = args[0], args[1:]
            length = self.data_handler.handle_list_set(key, vals)
            writer.write(self.builder.integer(length))

            # Wake up blocked BLPOP clients
            if key in self.server.blocks and self.server.blocks[key]:
                blocked_writer = self.server.blocks[key].popleft()
                if not self.server.blocks[key]:
                    del self.server.blocks[key]

                res = self.data_handler.handle_lpop(key, 1)
                if res:
                    response = self.builder.resp_array([key, res[0]])
                    blocked_writer.write(response)
                    await blocked_writer.drain()

        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_lpush(self, writer, args):
        try:
            key, vals = args[0], args[1:]
            length = self.data_handler.handle_list_set(key, vals, append=False)
            writer.write(self.builder.integer(length))
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_lrange(self, writer, args):
        try:
            key, start, end = args[0], int(args[1]), int(args[2])
            res = self.data_handler.handle_lrange(key, start, end)
            writer.write(self.builder.resp_array(res))
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_llen(self, writer, args):
        key = args[0]
        res = self.data_handler.handle_llen(key)
        writer.write(self.builder.integer(res))

    async def handle_lpop(self, writer, args):
        try:
            key = args[0]
            count = int(args[1]) if len(args) > 1 else 1
            res = self.data_handler.handle_lpop(key, count)

            if res is None or len(res) == 0:
                writer.write(self.builder.NULL_BULK_STR)
            elif len(res) == 1:
                writer.write(self.builder.bulk_str(res[0]))
            else:
                writer.write(self.builder.resp_array(res))
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_blpop(self, writer, args):
        try:
            key, timeout = args[0], float(args[1]) if len(args) > 1 else 0

            # Check if data is available immediately
            if self.data_handler.lists.get(key):
                res = self.data_handler.handle_lpop(key, 1)
                if res:
                    writer.write(self.builder.resp_array([key, res[0]]))
                    return

            # Block the client
            if key not in self.server.blocks:
                self.server.blocks[key] = deque()
            self.server.blocks[key].append(writer)

            # Set timeout
            if timeout > 0:
                await asyncio.sleep(timeout)
                if writer in self.server.blocks[key]:
                    self.server.blocks[key].remove(writer)
                    if not self.server.blocks[key]:
                        del self.server.blocks[key]
                    writer.write(self.builder.NULL_ARRAY)

        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

    async def handle_xadd(self, writer, args):
        try:
            key, stream_id, *fields = args
            val_id = self.data_handler.handle_xadd(key, stream_id, fields)
            writer.write(self.builder.bulk_str(val_id))

            # Wake up blocked XREAD clients - CRITICAL FIX
            if key in self.server.xread_blocks:
                # Create a copy of the list to avoid modification during iteration
                blocked_clients = self.server.xread_blocks[key][:]
                for blocked_info in blocked_clients:
                    blocked_writer, blocked_start_id = blocked_info

                    # For $, use the current last ID at time of XREAD
                    if blocked_start_id == '$':
                        blocked_start_id = self.data_handler.get_last_stream_id(key)

                    # Check if there are new entries
                    results = self.data_handler.handle_xread([key], [blocked_start_id])
                    if results and results[0][1]:  # If there are new entries
                        # Remove from blocked list
                        if blocked_info in self.server.xread_blocks[key]:
                            self.server.xread_blocks[key].remove(blocked_info)

                        # Send response
                        blocked_writer.write(self.builder.resp_array(results))
                        await blocked_writer.drain()

                # Clean up empty lists
                if not self.server.xread_blocks[key]:
                    del self.server.xread_blocks[key]

        except Exception as e:
            writer.write(self.builder.error(str(e)))

    async def handle_xread(self, writer, args):
        try:
            block_mode = False
            timeout_ms = 0
            streams_idx = 0

            # Parse arguments
            if args[0].upper() == "BLOCK":
                block_mode = True
                timeout_ms = int(args[1])
                streams_idx = 3 if len(args) > 2 and args[2].upper() == "STREAMS" else 2
            elif args[0].upper() == "STREAMS":
                streams_idx = 1

            # Extract keys and IDs
            remaining_args = args[streams_idx:]
            half = len(remaining_args) // 2
            keys = remaining_args[:half]
            ids = remaining_args[half:]

            # Handle $ for blocking - replace with current last ID
            processed_ids = []
            for key, id_val in zip(keys, ids):
                if id_val == '$' and block_mode:
                    processed_ids.append(self.data_handler.get_last_stream_id(key))
                else:
                    processed_ids.append(id_val)

            # Check for immediate results
            results = self.data_handler.handle_xread(keys, processed_ids, block_mode)
            has_data = any(entries for _, entries in results)

            if has_data or not block_mode:
                # Return immediately if we have data or not in block mode
                writer.write(self.builder.resp_array(results))
            else:
                # Block the client
                for key, original_id in zip(keys, ids):
                    if key not in self.server.xread_blocks:
                        self.server.xread_blocks[key] = []
                    self.server.xread_blocks[key].append((writer, original_id))

                # Set timeout if specified
                if timeout_ms > 0:
                    await asyncio.sleep(timeout_ms / 1000.0)

                    # Check if still blocked and no data arrived
                    still_blocked = any(
                        (writer, original_id) in self.server.xread_blocks.get(key, [])
                        for key, original_id in zip(keys, ids)
                    )

                    if still_blocked:
                        # Remove from blocked lists
                        for key, original_id in zip(keys, ids):
                            if key in self.server.xread_blocks:
                                if (writer, original_id) in self.server.xread_blocks[key]:
                                    self.server.xread_blocks[key].remove((writer, original_id))
                                if not self.server.xread_blocks[key]:
                                    del self.server.xread_blocks[key]

                        # Send null response
                        writer.write(self.builder.NULL_ARRAY)

        except Exception as e:
            print(f"XREAD error: {e}")
            writer.write(self.builder.error(str(e)))

    async def handle_xrange(self, writer, args):
        try:
            key, start_id, end_id = args[0], args[1], args[2]
            res = self.data_handler.handle_xrange(key, start_id, end_id)
            writer.write(self.builder.resp_array(res))
        except Exception as e:
            writer.write(self.builder.error(str(e)))

    async def handle_type(self, writer, args):
        try:
            key = args[0]
            if key in self.data_handler.strings:
                writer.write(b"+string\r\n")
            elif key in self.data_handler.lists:
                writer.write(b"+list\r\n")
            elif key in self.data_handler.streams:
                writer.write(b"+stream\r\n")
            else:
                writer.write(self.builder.NONE)
        except Exception as e:
            writer.write(self.builder.NULL_BULK_STR)

class AsyncRedisServer:
    def __init__(self):
        self.blocks = defaultdict(deque)  # For BLPOP
        self.xread_blocks = defaultdict(list)  # For XREAD

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request_handler = AsyncRequestHandler(self)

        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break

                await request_handler.handle_request(reader, writer, data)

        except Exception as e:
            print(f"Connection error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

async def main():
    server = AsyncRedisServer()
    service = await asyncio.start_server(
        server.handle_connection, 'localhost', 6379
    )

    print("Async Redis server running on localhost:6379")
    async with service:
        await service.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())