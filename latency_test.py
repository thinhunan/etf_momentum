#!/usr/bin/env python3
"""
从本机（中国上海）到全球主要互联网节点的延时对比测试。
使用 ping (ICMP) 测量 RTT；若不可用则回退到 TCP 443 连接时间。
"""

import subprocess
import socket
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# 全球主要节点： (区域描述, 主机名或 IP)
NODES = [
    # 中国大陆
    ("中国大陆-上海/华东", "www.baidu.com"),
    ("中国大陆-北京", "www.taobao.com"),
    # 港澳台
    ("中国香港", "www.hkex.com.hk"),
    ("中国台湾", "www.twse.com.tw"),
    # 亚太
    ("日本-东京", "www.yahoo.co.jp"),
    ("韩国-首尔", "www.naver.com"),
    ("新加坡", "www.singtel.com"),
    ("泰国-曼谷", "www.bangkokbank.com"),
    ("印尼-雅加达", "www.gojek.com"),
    ("沙特-利雅得", "www.aramco.com"),
    ("印度-孟买", "www.amazon.in"),
    ("澳大利亚-悉尼", "www.amazon.com.au"),
    # 北美
    ("美国-西海岸", "www.amazon.com"),
    ("美国-东海岸", "dns.google"),  # 8.8.8.8 的域名
    ("美国-谷歌DNS", "8.8.8.8"),
    ("加拿大", "www.amazon.ca"),
    # 欧洲
    ("英国-伦敦", "www.bbc.co.uk"),
    ("德国-法兰克福", "www.amazon.de"),
    ("法国-巴黎", "www.amazon.fr"),
    ("荷兰", "www.booking.com"),
    # 南美
    ("巴西-圣保罗", "www.amazon.com.br"),
]

PING_COUNT = 3
PING_TIMEOUT_SEC = 8
TCP_TIMEOUT_SEC = 6


def ping_rtt(host: str) -> float | None:
    """使用系统 ping 测 RTT（毫秒），失败返回 None。"""
    try:
        cmd = ["ping", "-c", str(PING_COUNT), "-W", str(PING_TIMEOUT_SEC), host]
        if sys.platform == "win32":
            cmd = ["ping", "-n", str(PING_COUNT), "-w", str(PING_TIMEOUT_SEC * 1000), host]
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=PING_TIMEOUT_SEC + 2,
        )
        if out.returncode != 0:
            return None
        # macOS/Linux: "round-trip min/avg/max/stddev = 12.345/23.456/..."
        m = re.search(r"min/avg/max[^=]*=\s*[\d.]+\/([\d.]+)\/", out.stdout)
        if not m:
            m = re.search(r"(\d+\.?\d*)\s*ms", out.stdout)
        if m:
            return float(m.group(1))
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        return None


def tcp_rtt(host: str, port: int = 443) -> float | None:
    """TCP 连接建立时间（毫秒），作为无 ping 时的备选。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TCP_TIMEOUT_SEC)
        start = __import__("time").time()
        s.connect((host, port))
        elapsed = (__import__("time").time() - start) * 1000
        s.close()
        return round(elapsed, 2)
    except Exception:
        return None


def resolve_host(host: str) -> str:
    """解析主机名到 IP（便于展示），解析失败返回原 host。"""
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", host):
        return host
    try:
        return socket.gethostbyname(host)
    except socket.gaierror:
        return host


def measure_one(region: str, host: str) -> tuple[str, str, float | None, str]:
    """测一个节点，返回 (区域, 主机, 延时ms, 方法)。"""
    ip = resolve_host(host)
    rtt = ping_rtt(host)
    method = "ping"
    if rtt is None:
        rtt = tcp_rtt(host)
        method = "tcp"
    return (region, host, rtt, method)


def main():
    print("=" * 60)
    print("  从本机（中国上海）到全球主要节点延时测试")
    print("=" * 60)

    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(measure_one, r, h): (r, h) for r, h in NODES}
        for fut in as_completed(futures):
            region, host, rtt, method = fut.result()
            results.append((region, host, rtt, method))

    # 按延时排序，None 放最后
    results.sort(key=lambda x: (x[2] is None, x[2] if x[2] is not None else 9999))

    print(f"\n{'区域':<20} {'主机':<24} {'延时(ms)':<10} 方法")
    print("-" * 60)
    for region, host, rtt, method in results:
        rtt_str = f"{rtt:.1f}" if rtt is not None else "超时/失败"
        host_show = host if len(host) <= 22 else host[:19] + "..."
        print(f"{region:<20} {host_show:<24} {rtt_str:<10} {method}")

    ok = [r for r in results if r[2] is not None]
    if ok:
        avg = sum(r[2] for r in ok) / len(ok)
        best = min(r[2] for r in ok)
        worst = max(r[2] for r in ok)
        print("-" * 60)
        print(f"统计: 成功 {len(ok)}/{len(results)}, 最小 {best:.1f} ms, 最大 {worst:.1f} ms, 平均 {avg:.1f} ms")
    print("=" * 60)


if __name__ == "__main__":
    main()
