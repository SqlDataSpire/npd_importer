"""A local imitation of directory.cms.gov: public URLs 302 to short-lived 'signed' URLs that support Range."""
from __future__ import annotations

import hashlib
import itertools
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from release_builder import BuiltRelease


class FakeCms:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.manifests: list[bytes] = []
        self.drops: dict[str, list[int]] = {}
        self.ignore_range: set[str] = set()
        self.expire_next: set[str] = set()
        self.requests: list[tuple[str, str, str | None]] = []
        self._tokens: dict[str, tuple[str, bool]] = {}
        self._counter = itertools.count(1)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def manifest_url(self) -> str:
        return self.base_url + "/downloads/manifest.json"

    def public_url(self, name: str) -> str:
        return f"{self.base_url}/downloads/{name}"

    def start(self) -> "FakeCms":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def publish(self, release: BuiltRelease, later_manifest: bytes | None = None) -> None:
        """Serve `release`. If `later_manifest` is given, every manifest GET after the first returns it."""
        self.objects = dict(release.files)
        self.manifests = [release.manifest] + ([later_manifest] if later_manifest else [])

    # ---- internals used by the handler ---------------------------------
    def _issue(self, name: str) -> str:
        token = str(next(self._counter))
        valid = name not in self.expire_next
        self.expire_next.discard(name)
        self._tokens[token] = (name, valid)
        return token

    def _valid(self, name: str, token: str) -> bool:
        return self._tokens.get(token) == (name, True)

    def _body(self, name: str) -> bytes | None:
        if name == "manifest.json":
            body = self.manifests[0]
            if len(self.manifests) > 1:
                self.manifests.pop(0)
            return body
        return self.objects.get(name)


def _handler(cms: FakeCms) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            pass

        def _empty(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_HEAD(self) -> None:
            self._empty(403)  # like S3 signed GET URLs

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            cms.requests.append(("GET", url.path, self.headers.get("Range")))
            if url.path.startswith("/downloads/"):
                name = url.path.removeprefix("/downloads/")
                if name != "manifest.json" and name not in cms.objects:
                    return self._empty(404)
                self.send_response(302)
                self.send_header("Location", f"/s3/{name}?token={cms._issue(name)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if url.path.startswith("/s3/"):
                name = url.path.removeprefix("/s3/")
                if not cms._valid(name, parse_qs(url.query).get("token", [""])[0]):
                    return self._empty(403)
                body = cms._body(name)
                if body is None:
                    return self._empty(404)
                return self._send(name, body)
            self._empty(404)

        def _send(self, name: str, body: bytes) -> None:
            start = 0
            rng = self.headers.get("Range")
            if rng and name not in cms.ignore_range:
                start = int(rng.removeprefix("bytes=").split("-")[0])
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{len(body) - 1}/{len(body)}")
            else:
                self.send_response(200)
            part = body[start:]
            self.send_header("Content-Length", str(len(part)))
            self.send_header("Last-Modified", "Tue, 29 Sep 2026 04:00:00 GMT")
            self.send_header("ETag", f'"{hashlib.md5(body).hexdigest()}"')
            self.end_headers()
            drops = cms.drops.get(name)
            if drops:
                cut = drops.pop(0)
                self.wfile.write(part[:cut])
                self.wfile.flush()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            self.wfile.write(part)

    return Handler
