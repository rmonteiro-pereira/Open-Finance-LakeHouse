"""A polite HTTP client: identifies itself, obeys robots.txt, paces requests per host."""

from __future__ import annotations

import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

USER_AGENT = (
    "ofl-documents-archive/0.1 (personal research archive; "
    "+https://github.com/rmonteiro-pereira/Open-Finance-LakeHouse)"
)


class RobotsDisallowed(Exception):
    pass


class PoliteClient:
    def __init__(self, *, min_interval: float = 3.0, timeout: float = 60.0, session=None, sleep=time.sleep):
        self.min_interval = min_interval
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self._sleep = sleep
        self._last: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser | None] = {}

    def _robots_for(self, scheme: str, host: str) -> RobotFileParser | None:
        if host not in self._robots:
            parser: RobotFileParser | None = RobotFileParser()
            try:
                resp = self.session.get(f"{scheme}://{host}/robots.txt", timeout=self.timeout)
                if resp.status_code == 200 and "text" in resp.headers.get("content-type", "text"):
                    parser.parse(resp.text.splitlines())
                else:  # no robots file: nothing is disallowed
                    parser = None
            except requests.RequestException:
                parser = None
            self._robots[host] = parser
        return self._robots[host]

    def _wait(self, host: str, robots: RobotFileParser | None) -> None:
        interval = self.min_interval
        if robots is not None:
            delay = robots.crawl_delay(USER_AGENT) or robots.crawl_delay("*")
            if delay:
                interval = max(interval, float(delay))
        elapsed = time.monotonic() - self._last.get(host, 0.0)
        if host in self._last and elapsed < interval:
            self._sleep(interval - elapsed)

    def get(self, url: str, **kwargs) -> requests.Response:
        parts = urlparse(url)
        host = parts.netloc
        robots = self._robots_for(parts.scheme, host)
        if robots is not None and not robots.can_fetch(USER_AGENT, url):
            raise RobotsDisallowed(url)
        self._wait(host, robots)
        try:
            return self.session.get(url, timeout=self.timeout, **kwargs)
        finally:
            self._last[host] = time.monotonic()
