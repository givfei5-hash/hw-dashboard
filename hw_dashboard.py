#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hw_dashboard.py —— 推理服务器硬件 + Strata 引擎状态看板
纯 Python 标准库，不依赖 psutil / 第三方包（服务器无外网）。

端口 8092   数据 <部署目录>/hw_history.db
只读采集，不启停任何服务。
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "hw_history.db"
STATIC_CACHE = {}

# ---------------- 可调参数 ----------------
PORT = int(os.environ.get("HW_DASH_PORT", "8092"))
SAMPLE_INTERVAL = float(os.environ.get("HW_DASH_SAMPLE", "3"))     # 采集周期（秒）
# GPU 出力快采周期（秒）：串行流水线一张卡只亮几百毫秒，3s 主采样会整个错过。
# 设 0 关闭这条通道（前端自动退回 3s 通道）。
GPU_FAST_INTERVAL = float(os.environ.get("HW_DASH_GPU_FAST", "0.7"))
STORE_INTERVAL = float(os.environ.get("HW_DASH_STORE", "5"))       # 落库周期（秒）
GPU_STORE_INTERVAL = float(os.environ.get("HW_DASH_GPU_STORE", "15"))
RETAIN_DAYS = int(os.environ.get("HW_DASH_RETAIN", "14"))
GPU_RETAIN_DAYS = int(os.environ.get("HW_DASH_GPU_RETAIN", "7"))
ENGINE_URL = os.environ.get("HW_DASH_ENGINE", "http://127.0.0.1:8000")   # 预算代理（本地免 key）
NET_IFACES = [s for s in os.environ.get("HW_DASH_NIC", "enp40s0").split(",") if s]

# 阈值：warn / crit
# 2026-11-04 按 13.4h 实测分布重定（八卡 T10+A5500、Strata 常驻在服）：
#   旧值 gpu_temp 85/92、cpu_temp 80/90、cpu_pct 92/99 三件在这台机器上从来没响过，
#   实测 GPU 峰温 65°C（A5500）/ 57°C（T10）、CPU 封装 41°C、CPU 利 48%，
#   以“正常运行上沿 + 余量”重定；下面括号里是实测参考值。
TH = {
    "gpu_temp":    (70, 78),       # 实测：每卡峰 65 / p99 57 / 均温 p99 47
    "cpu_temp":    (70, 82),       # 实测：p99 37 / 峰 41（22C44T，两代机器同用）
    "nvme_temp":   (68, 75),
    "disk_pct":    None,  # 已废弃：磁盘水位告警 11-06 整条删除（留位免得索引错位）
    "mem_avail_gb": (32, 20),      # 注意：低于阈值才告警。实测稳在 47~48 GB
    "swap_used_gb": (6, 12),       # 实测：稳态 2.6 GB / p99 2.60
    "cpu_pct":     (85, 95),       # 实测：p99 47 / 峰 48，本机 CPU 不是瓶颈
}
EVENT_COOLDOWN = 300
# “状态型”事件（功耗墙、降频）：进了异常态就别每 5 分钟复读，半小时内只说一次
EVENT_COOLDOWN_LONG = 1800
# 功耗墙判据：nvidia-smi 瞬时读数会抖到上限以上（实测最高 175W / 上限 150W），
# 所以“差 2% 到上限”不算事，要超得明显且连续几拍才报。
PCAP_RATIO = 1.08      # 功率达到上限的 108% 起计
PCAP_STREAK = 3        # 连续 3 拍（≈ 9s）才报，滤掉单点抖动

HOST = os.uname().nodename
HZ = os.sysconf("SC_CLK_TCK")


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ============================ 数据库 ============================
def db_init():
    # check_same_thread=False：采集线程与 HTTP 线程共用，所有访问由 DB_LOCK 保护
    c = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS snap(
          ts REAL PRIMARY KEY,
          cpu REAL,
          load1 REAL, load5 REAL, load15 REAL, load_norm REAL,
          mem_used REAL, mem_total REAL, mem_avail REAL, cached REAL,
          swap_used REAL, swap_total REAL,
          strata_rss REAL,
          gpu_util REAL, gpu_mem REAL, gpu_mem_total REAL,
          gpu_temp REAL, gpu_temp_max REAL, gpu_power REAL, gpu_plimit REAL,
          pcie_rx REAL,
          net_rx REAL, net_tx REAL,
          disk_r REAL, disk_w REAL,
          cpu_temp REAL,
          tok_s REAL, prefill_tok_s REAL,
          eng_up INTEGER, queued INTEGER, gen INTEGER,
          psi_cpu REAL, psi_io REAL, cores_json TEXT
        );
        CREATE TABLE IF NOT EXISTS gpub(
          ts REAL, i INTEGER,
          util REAL, mem REAL, temp REAL, power REAL, clk REAL,
          PRIMARY KEY(ts, i)
        );
        CREATE TABLE IF NOT EXISTS req(
          id TEXT PRIMARY KEY, ts REAL, dur REAL,
          prompt INTEGER, reused INTEGER, out INTEGER,
          ttft REAL, prompt_ms REAL, decode_ms REAL, decode_toks REAL,
          hit REAL, drafts INTEGER, acc INTEGER, finish TEXT, engine TEXT
        );
        CREATE TABLE IF NOT EXISTS evt(
          ts REAL, key TEXT, level TEXT, text TEXT
        );
        CREATE INDEX IF NOT EXISTS ix_req_ts ON req(ts);
        CREATE INDEX IF NOT EXISTS ix_evt_ts ON evt(ts);
        """
    )
    c.commit()
    # 旧库升级：补上功率上限列（已存在则忽略）
    try:
        c.execute("ALTER TABLE snap ADD COLUMN gpu_plimit REAL")
        c.commit()
    except sqlite3.OperationalError:
        pass
    return c


DB = db_init()
DB_LOCK = threading.Lock()


def db_prune():
    now = time.time()
    with DB_LOCK:
        DB.execute("DELETE FROM snap WHERE ts < ?", (now - RETAIN_DAYS * 86400,))
        DB.execute("DELETE FROM gpub WHERE ts < ?", (now - GPU_RETAIN_DAYS * 86400,))
        DB.execute("DELETE FROM req WHERE ts < ?", (now - RETAIN_DAYS * 86400,))
        DB.execute("DELETE FROM evt WHERE ts < ?", (now - RETAIN_DAYS * 86400,))
        DB.commit()


# ============================ 事件 ============================
EVENT_STATE = {}   # key -> last_ts
LATEST_EVENTS = []
PCAP_HIT = {}      # "pcap{i}" -> 连续超功耗上限的拍数
THR_STATE = {}     # i -> 上一次报出的降频位集合（用来只报跳变）


def emit(key, level, text, cooldown=None):
    now = time.time()
    if now - EVENT_STATE.get(key, 0) < (cooldown or EVENT_COOLDOWN):
        return
    EVENT_STATE[key] = now
    LATEST_EVENTS.append({"ts": now, "key": key, "level": level, "text": text})
    del LATEST_EVENTS[:-200]
    with DB_LOCK:
        DB.execute("INSERT INTO evt VALUES(?,?,?,?)", (now, key, level, text))
        DB.commit()


def check_threshold(key, value, pair, fmt, invert=False, cooldown=None):
    """invert=True 表示值越小越危险"""
    if value is None:
        return
    warn, crit = pair
    if invert:
        lvl = "crit" if value <= crit else ("warn" if value <= warn else None)
    else:
        lvl = "crit" if value >= crit else ("warn" if value >= warn else None)
    if lvl:
        emit(f"{key}:{lvl}", lvl, fmt.format(v=value), cooldown=cooldown)


# ============================ 采集 ============================
def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except Exception:
        return ""


BTIME = 0
for _ln in read_file("/proc/stat").splitlines():
    if _ln.startswith("btime"):
        BTIME = int(_ln.split()[1])


def jcpu():
    """返回 (总利用率, 各核利用率列表, 空闲率)"""
    lines = read_file("/proc/stat").splitlines()
    per = {}
    for ln in lines:
        if not ln.startswith("cpu"):
            continue
        parts = ln.split()
        name = parts[0]
        v = [int(x) for x in parts[1:]]
        idle = v[3] + (v[4] if len(v) > 4 else 0)
        total = sum(v)
        per[name] = (total, idle)
    out_total, out_cores = None, []
    prev = STATIC_CACHE.get("cpu_prev")
    if prev:
        if "cpu" in per and "cpu" in prev:
            dt = per["cpu"][0] - prev["cpu"][0]
            di = per["cpu"][1] - prev["cpu"][1]
            if dt > 0:
                out_total = round(100.0 * (1 - di / dt), 1)
        for name, (t, i) in per.items():
            if name == "cpu" or name not in prev:
                continue
            dt = t - prev[name][0]
            di = i - prev[name][1]
            idx = int(name[3:])
            val = round(100.0 * (1 - di / dt), 1) if dt > 0 else 0.0
            out_cores.append((idx, val))
    STATIC_CACHE["cpu_prev"] = per
    out_cores.sort()
    return out_total, [c[1] for c in out_cores]


def jmem():
    m = {}
    for ln in read_file("/proc/meminfo").splitlines():
        k, _, rest = ln.partition(":")
        m[k] = int(rest.strip().split()[0]) * 1024      # kB -> B
    total = m.get("MemTotal", 0)
    avail = m.get("MemAvailable", 0)
    return {
        "total": total,
        "avail": avail,
        "used": total - avail,
        "cached": m.get("Cached", 0) + m.get("SReclaimable", 0),
        "buffers": m.get("Buffers", 0),
        "swap_total": m.get("SwapTotal", 0),
        "swap_used": m.get("SwapTotal", 0) - m.get("SwapFree", 0),
        "dirty": m.get("Dirty", 0),
        # 按 free 口径的活跃占用（刨掉可回收缓存）
        "app": total - m.get("MemFree", 0) - m.get("Cached", 0) - m.get("Buffers", 0) - m.get("SReclaimable", 0),
    }


def find_strata():
    """找推理引擎进程：RSS + 运行时长 + 命令行（strata / maya 两种框架都认）。
    strata 的 comm 是 strata/strata-host；maya 跑在 venv 的 python 里，
    comm 是 python，只能靠命令行里的 serve/server.py + maya 认。
    进程找不到时 rss=0，内存图那条「引擎 RSS」线会塌到 0，
    纵轴整条被拉爆——这是 11-06 换 maya 后内存图「又不显示」的根因。"""
    out = {"rss": 0, "pid": None, "etime_s": None, "cmd": "",
           "vision_rss": 0, "procs": 0, "kind": None}
    page = os.sysconf("SC_PAGE_SIZE")
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        comm = read_file(f"/proc/{pid}/comm").strip()
        cmd = ""
        if comm.startswith("strata"):
            kind = "strata"
        else:
            if not comm.startswith(("python", "server")):
                continue
            cmd = read_file(f"/proc/{pid}/cmdline").replace("\0", " ").strip()
            if "serve/server.py" not in cmd or "maya" not in cmd:
                continue
            kind = "maya"
        try:
            statm = read_file(f"/proc/{pid}/statm").split()
            rss = int(statm[1]) * page
            if not cmd:
                cmd = read_file(f"/proc/{pid}/cmdline").replace("\0", " ").strip()
            st = read_file(f"/proc/{pid}/stat").split()
            starttime_ticks = int(st[21])
            proc_start = BTIME + starttime_ticks / float(HZ)
            if kind == "strata" and "vision" in comm:
                out["vision_rss"] += rss
                out["procs"] += 1
                continue
            out["rss"] += rss
            out["procs"] += 1
            if out["pid"] is None or rss > out.get("_r", 0):
                out["_r"] = rss
                out["pid"] = int(pid)
                out["etime_s"] = time.time() - proc_start
                out["cmd"] = cmd
                out["kind"] = kind
        except Exception:
            continue
    out.pop("_r", None)
    return out


GPU_FIELDS = ("index,name,uuid,serial,pstate,temperature.gpu,power.draw,power.limit,"
              "power.max_limit,memory.used,memory.total,utilization.gpu,utilization.memory,"
              "clocks.sm,clocks.max.sm,fan.speed,pcie.link.gen.current,pcie.link.width.current,"
              "driver_version,vbios_version,compute_mode,persistence_mode,"
              "clocks_throttle_reasons.active,enforced.power.limit")
# 末尾那个 enforced.power.limit 是必需的后备：RTX A5500 Laptop GPU 的 power.limit 报 [N/A]，
# 只报 enforced.power.limit=115W。不取它，这张卡的功率上限就是空，
# 功耗墙告警对它就永不生效，而且聚合的「功率占上限」会分子含它、分母不含它。

# clocks_throttle_reasons.active 位定义
# 硬降频（驱动因硬件限制主动压频）值得告警
THROTTLE_HARD = [
    (0x8, "硬件降频"),
    (0x10, "温度墙"),
    (0x20, "功率刹车"),
    (0x100, "软件温度降频"),
]
# 软约束：在 T10 这类被动卡上，DVFS 也会置位，当信息展示就好
THROTTLE_SOFT = [
    (0x4, "功耗上限"),
]


def jgpu():
    try:
        r = subprocess.run(
            ["nvidia-smi", f"--query-gpu={GPU_FIELDS}", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            return None, r.stderr.strip()[:200]
    except Exception as e:
        return None, str(e)[:200]
    gpus = []
    for ln in r.stdout.strip().splitlines():
        f = [x.strip() for x in ln.split(",")]
        if len(f) < 24:
            continue

        def num(i):
            try:
                return float(f[i])
            except Exception:
                return None

        def txt(i):
            v = f[i]
            return None if v in ("[N/A]", "N/A", "[Not Supported]", "") else v

        try:
            mask = int(f[22], 16)
        except Exception:
            mask = 0
        thr = [name for bit, name in THROTTLE_HARD if mask & bit]
        thr_soft = [name for bit, name in THROTTLE_SOFT if mask & bit]
        gpus.append({
            "i": int(num(0) if num(0) is not None else len(gpus)),
            "name": f[1],
            "uuid": f[2],
            "serial": txt(3),
            "pstate": txt(4),
            "temp": num(5),
            "power": num(6),
            "plimit": num(7) if num(7) is not None else num(23),   # power.limit 为 N/A 时退到 enforced
            "pmax": num(8),
            "mem_used": num(9),
            "mem_total": num(10),
            "util": num(11),
            "mem_util": num(12),
            "clk": num(13),
            "clk_max": num(14),
            "fan": num(15),
            "pcie_gen": num(16),
            "pcie_width": num(17),
            "driver": txt(18),
            "vbios": txt(19),
            "compute": txt(20),
            "persist": txt(21),
            "throttle": thr,
            "throttle_soft": thr_soft,
            "throttle_mask": mask,
        })
    return (gpus if gpus else None), None


def jdisk():
    """返回 {dev: (read_bytes_total, write_bytes_total, busy_ms)} 与速率"""
    cur = {}
    for ln in read_file("/proc/diskstats").splitlines():
        p = ln.split()
        if len(p) < 14:
            continue
        dev = p[2]
        if dev.startswith("nvme") and "p" not in dev[4:]:
            cur[dev] = (int(p[6]) * 512, int(p[10]) * 512, int(p[12]))
        elif dev.startswith("sd") and len(dev) == 4:
            cur[dev] = (int(p[6]) * 512, int(p[10]) * 512, int(p[12]))
    prev = STATIC_CACHE.get("disk_prev")
    now = STATIC_CACHE.get("disk_ts", 0)
    dt = time.time() - now if prev else 0
    rates = {}
    if prev and dt > 0:
        for dev, (r, w, b) in cur.items():
            if dev in prev:
                pr, pw, pb = prev[dev]
                rates[dev] = {
                    "r": max(0.0, (r - pr) / dt),
                    "w": max(0.0, (w - pw) / dt),
                    "util": clamp(100.0 * (b - pb) / (dt * 1000.0), 0, 100),
                }
    STATIC_CACHE["disk_prev"] = cur
    STATIC_CACHE["disk_ts"] = time.time()
    return rates


def jmounts():
    mounts = []
    seen = set()
    for ln in read_file("/proc/mounts").splitlines():
        p = ln.split()
        if len(p) < 3:
            continue
        dev, mnt, fstype = p[0], p[1], p[2]
        if fstype not in ("ext4", "xfs", "btrfs") or not dev.startswith("/dev/"):
            continue
        if mnt in seen:
            continue
        seen.add(mnt)
        try:
            st = os.statvfs(mnt)
        except Exception:
            continue
        total = st.f_blocks * st.f_frsize
        if total <= 0:
            continue
        free = st.f_bavail * st.f_frsize
        used = total - st.f_bfree * st.f_frsize
        mounts.append({
            "dev": dev.split("/")[-1], "mnt": mnt, "fstype": fstype,
            "total": total, "used": used, "free": free,
            "pct": round(100.0 * used / total, 1),
        })
    mounts.sort(key=lambda m: m["mnt"])
    return mounts


def jnet():
    cur = {}
    for ln in read_file("/proc/net/dev").splitlines()[2:]:
        name, _, rest = ln.partition(":")
        name = name.strip()
        if NET_IFACES and name not in NET_IFACES:
            continue
        f = rest.split()
        if len(f) < 16:
            continue
        cur[name] = (int(f[0]), int(f[8]))
    prev = STATIC_CACHE.get("net_prev")
    now = time.time()
    dt = now - STATIC_CACHE.get("net_ts", 0)
    out = {}
    if prev and dt > 0:
        for n, (rx, tx) in cur.items():
            if n in prev:
                out[n] = {"rx": max(0.0, (rx - prev[n][0]) / dt),
                          "tx": max(0.0, (tx - prev[n][1]) / dt),
                          "rx_total": rx, "tx_total": tx}
    STATIC_CACHE["net_prev"] = cur
    STATIC_CACHE["net_ts"] = now
    return out


def jtemps():
    out = {}
    for z in Path("/sys/class/thermal").glob("thermal_zone*"):
        try:
            t = (z / "type").read_text().strip()
            v = int((z / "temp").read_text().strip()) / 1000.0
            if t == "x86_pkg_temp":
                out["cpu"] = v
        except Exception:
            continue
    # NVMe 温度
    nv = []
    for h in Path("/sys/class/hwmon").glob("hwmon*"):
        try:
            if (h / "name").read_text().strip() != "nvme":
                continue
            nv.append(round(int((h / "temp1_input").read_text().strip()) / 1000.0, 1))
        except Exception:
            continue
    if nv:
        out["nvme_max"] = max(nv)
        out["nvme"] = nv
    # 机箱风扇
    fan = read_file("/sys/class/hwmon/hwmon2/fan2_input").strip()
    if fan.isdigit():
        out["fan_case"] = int(fan)
    return out


def jpsi():
    out = {}
    for k in ("cpu", "memory", "io"):
        txt = read_file(f"/proc/pressure/{k}")
        for ln in txt.splitlines():
            if ln.startswith("some"):
                for kv in ln.split()[1:]:
                    a, _, v = kv.partition("=")
                    if a == "avg10":
                        out[k] = float(v)
    return out


def http_json(url, timeout=2.5, key=None):
    req = urllib.request.Request(url)
    if key:
        req.add_header("Authorization", f"Bearer {key}")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def strata_api_key():
    if "api_key" in STATIC_CACHE:
        return STATIC_CACHE["api_key"]
    k = ""
    try:
        for p in (Path.home()/".local/state/strata/launch").glob("*.json"):
            if ".bak" in p.name or ".choices" in p.name:
                continue
            try:
                d = json.loads(p.read_text())
            except Exception:
                continue
            if isinstance(d, dict) and d.get("api_key"):
                k = d["api_key"]
                break
    except Exception:
        pass
    STATIC_CACHE["api_key"] = k
    return k


def jengine():
    """引擎指标：优先走预算代理(本地免 key)，失败再直连 8080 带 key"""
    errs = []
    for url, use_key in ((ENGINE_URL, False), ("http://127.0.0.1:8080", True)):
        try:
            m = http_json(url.rstrip("/") + "/metrics", timeout=3,
                          key=strata_api_key() if use_key else None)
            m["_via"] = "proxy" if not use_key else "direct"
            # 框架标记：maya 的 engine 段带 maya_version / engine_kind，strata 都没有。
            # 前端靠它决定标签文案和引擎配置卡用哪套字段。
            m["_kind"] = "maya" if ((m.get("engine") or {}).get("maya_version")
                                    or (m.get("engine") or {}).get("engine_kind")) else "strata"
            return m, errs
        except Exception as e:
            errs.append(f"{url}: {type(e).__name__}")
    return None, errs


def collect():
    ts = time.time()
    cpu_pct, cores = jcpu()
    mem = jmem()
    st = find_strata()
    gpus, gpu_err = jgpu()
    disks = jdisk()
    mounts = jmounts()
    net = jnet()
    temps = jtemps()
    psi = jpsi()
    eng, eng_err = jengine()

    la = [float(x) for x in read_file("/proc/loadavg").split()[:3]]
    up = 0.0
    try:
        up = float(read_file("/proc/uptime").split()[0])
    except Exception:
        pass

    nproc = os.cpu_count() or 1
    freqs = []
    for f in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_cur_freq"):
        v = read_file(f).strip()
        if v.isdigit():
            freqs.append(int(v) / 1000.0)
    freq_avg = round(sum(freqs) / len(freqs), 0) if freqs else None

    g = {
        "util": round(sum(x["util"] or 0 for x in gpus) / len(gpus), 1) if gpus else None,
        "mem_used": round(sum(x["mem_used"] or 0 for x in gpus)) if gpus else None,
        "mem_total": round(sum(x["mem_total"] or 0 for x in gpus)) if gpus else None,
        "temp": round(sum(x["temp"] or 0 for x in gpus) / len(gpus), 1) if gpus else None,
        "temp_max": max([x["temp"] or 0 for x in gpus]) if gpus else None,
        "power": round(sum(x["power"] or 0 for x in gpus), 1) if gpus else None,
        "plimit": round(sum(x["plimit"] or 0 for x in gpus), 1) if gpus else None,
    } if gpus else {"util": None, "mem_used": None, "mem_total": None,
                    "temp": None, "temp_max": None, "power": None, "plimit": None}

    live = (eng or {}).get("live") or {}
    hw = (eng or {}).get("hardware") or {}
    dr = sum(v["r"] for v in disks.values()) if disks else 0.0
    dw = sum(v["w"] for v in disks.values()) if disks else 0.0

    snap = {
        "ts": ts,
        "cpu": cpu_pct,
        "cpu0": la[0] if la else None,
        "cpu1": la[1] if len(la) > 1 else None,
        "load1": la[0], "load5": la[1], "load15": la[2],
        "load_norm": round(la[0] / nproc * 100, 1) if la else None,
        "cpu_temp": temps.get("cpu"),
        "freq": freq_avg,
        "cores": cores,
        "nproc": nproc,
        "psi": psi,
        "mem": mem,
        "mem_pct": round(100.0 * mem["used"] / mem["total"], 1) if mem["total"] else None,
        "strata": st,
        # 引擎侧内存（maya/strata 都报 hardware.ram_used，字节）；进程 RSS 兕底。
        # 内存图第二条线与 MAYA/STRATA 常驻 KPI 走这个值：maya 的专家缓存在页缓存里，
        # 进程 RSS 只有 0.4G，画出来没意义（Strata 时代是 196G，能画出型来）。
        "eng_ram": hw.get("ram_used") or (st["rss"] or None),
        "gpus": gpus,
        "g": g,
        "gpu_err": gpu_err,
        "disk_rates": disks,
        "disk_r": dr, "disk_w": dw,
        "mounts": mounts,
        "nvme_temp": temps.get("nvme_max"),
        "fan_case": temps.get("fan_case"),
        "net": net,
        "uptime": up,
        "engine": eng,
        "engine_err": eng_err,
        "eng_up": bool(eng),
        "queued": live.get("queued"),
        "gen": live.get("generated"),
        "tok_s": hw.get("tok_s"),
        "prefill_tok_s": hw.get("prefill_tok_s_mean"),
    }

    # ---- 告警 ----
    if gpus:
        for x in gpus:
            if x["temp"] is not None:
                check_threshold("gpu_temp", x["temp"], TH["gpu_temp"],
                                f"GPU{x['i']} 温度 {{v:.0f}}°C")
            # 功耗墙：连续 PCAP_STREAK 拍超 PCAP_RATIO 才报，恢复后重新计数
            k = f"pcap{x['i']}"
            if x["plimit"] and x["power"] and x["power"] >= x["plimit"] * PCAP_RATIO:
                PCAP_HIT[k] = PCAP_HIT.get(k, 0) + 1
                if PCAP_HIT[k] >= PCAP_STREAK:
                    emit(f"{k}:warn", "warn",
                         f"GPU{x['i']} 持续超功耗上限 {x['power']:.0f}W"
                         f"（上限 {x['plimit']:.0f}W，达 {100 * x['power'] / x['plimit']:.0f}%"
                         f"，连续 {PCAP_HIT[k]} 拍）",
                         cooldown=EVENT_COOLDOWN_LONG)
            else:
                PCAP_HIT.pop(k, None)
            # 降频：只在状态跳变时报（进入 / 解除），常驻现象不再每五分钟复读
            thr_now = frozenset(x["throttle"] or ())
            thr_was = THR_STATE.get(x["i"], frozenset())
            if thr_now != thr_was:
                if thr_now:
                    emit(f"thr{x['i']}:warn", "warn",
                         f"GPU{x['i']} 进入降频：{'/'.join(sorted(thr_now))}",
                         cooldown=EVENT_COOLDOWN_LONG)
                else:
                    emit(f"thr{x['i']}:ok", "info",
                         f"GPU{x['i']} 降频解除", cooldown=EVENT_COOLDOWN_LONG)
                THR_STATE[x["i"]] = thr_now
    if temps.get("cpu") is not None:
        check_threshold("cpu_temp", temps["cpu"], TH["cpu_temp"], "CPU 封装温度 {v:.0f}°C")
    if temps.get("nvme_max") is not None:
        check_threshold("nvme_temp", temps["nvme_max"], TH["nvme_temp"], "NVMe 温度 {v:.0f}°C")
    # 磁盘水位告警已删（11-06 小蜜蜂拍板「没意义，不留」）：状态型水位只会复读，
    # footer 卡上各盘百分比常驻可见，真满了人自己看得见。TH 里 disk_pct 一并撤。
    check_threshold("mem_avail", mem["avail"] / 2**30, TH["mem_avail_gb"],
                    "可用内存 {v:.1f} GB", invert=True)
    check_threshold("swap", mem["swap_used"] / 2**30, TH["swap_used_gb"],
                    "交换分区已用 {v:.1f} GB")
    if cpu_pct is not None:
        check_threshold("cpu", cpu_pct, TH["cpu_pct"], "CPU 利用率 {v:.0f}%")
    if not eng:
        emit("eng:crit", "crit", "推理引擎无响应：" + " / ".join(eng_err))
    if gpu_err:
        emit("gpu:crit", "crit", f"nvidia-smi 取数失败：{gpu_err}")
    return snap


# ============================ 落库 ============================
def store(snap):
    ts = snap["ts"]
    m = snap["mem"]
    g = snap["g"]
    with DB_LOCK:
        cols = ("ts,cpu,load1,load5,load15,load_norm,mem_used,mem_total,mem_avail,cached,"
                "swap_used,swap_total,strata_rss,gpu_util,gpu_mem,gpu_mem_total,gpu_temp,"
                "gpu_temp_max,gpu_power,gpu_plimit,pcie_rx,net_rx,net_tx,disk_r,disk_w,"
                "cpu_temp,tok_s,prefill_tok_s,eng_up,queued,gen,psi_cpu,psi_io,cores_json")
        DB.execute(
            f"INSERT OR REPLACE INTO snap ({cols}) VALUES(" + ",".join(["?"] * 34) + ")",
            (ts, snap["cpu"],
             snap["load1"], snap["load5"], snap["load15"], snap["load_norm"],
             m["used"], m["total"], m["avail"], m["cached"],
             m["swap_used"], m["swap_total"],
             snap.get("eng_ram") or snap["strata"]["rss"],
             g["util"], g["mem_used"], g["mem_total"],
             g["temp"], g["temp_max"], g["power"], g["plimit"],
             (snap["engine"] or {}).get("hardware", {}).get("gpu_pcie_rx_mb"),
             sum(v["rx"] for v in snap["net"].values()) if snap["net"] else 0,
             sum(v["tx"] for v in snap["net"].values()) if snap["net"] else 0,
             snap["disk_r"], snap["disk_w"],
             snap["cpu_temp"],
             snap["tok_s"], snap["prefill_tok_s"],
             1 if snap["eng_up"] else 0, snap["queued"], snap["gen"],
             snap["psi"].get("cpu"), snap["psi"].get("io"),
             json.dumps(snap["cores"])))
        DB.commit()


def store_gpu(snap):
    if not snap["gpus"]:
        return
    ts = snap["ts"]
    with DB_LOCK:
        for x in snap["gpus"]:
            DB.execute("INSERT OR REPLACE INTO gpub VALUES(?,?,?,?,?,?,?)",
                       (ts, x["i"], x["util"], x["mem_used"], x["temp"], x["power"], x["clk"]))
        DB.commit()


def store_reqs(snap):
    eng = snap["engine"]
    if not eng:
        return
    with DB_LOCK:
        for r in (eng.get("requests") or []):
            # maya 的请求记录没有 id 字段，用时间+prompt 合成一个稳定 id，
            # 否则 INSERT OR IGNORE 全部跳过，请求历史永远空。
            rid = r.get("id") or "%.6f-%s" % (r.get("time") or 0, r.get("prompt_tokens"))
            if not rid:
                continue
            DB.execute("INSERT OR IGNORE INTO req VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (rid, r.get("time"), r.get("duration_s"),
                        r.get("prompt_tokens"), r.get("reused"), r.get("output_tokens"),
                        r.get("first_engine_token_s"), r.get("prompt_ms"), r.get("decode_ms"),
                        r.get("decode_tok_s"), r.get("hit_rate"),
                        r.get("drafts_offered"), r.get("drafts_accepted"),
                        r.get("finish"), snap["ts"]))
        DB.commit()


# ============================ 历史查询 ============================
def downsample(rows, target=360):
    """rows: [(ts, v1, v2, ...)] -> 分桶平均"""
    if not rows:
        return {"ts": [], "series": []}
    ncol = len(rows[0]) - 1
    step = max(1, len(rows) // target)
    out_ts, buckets = [], [[] for _ in range(ncol)]
    i = 0
    while i < len(rows):
        chunk = rows[i:i + step]
        out_ts.append(chunk[len(chunk) // 2][0])
        for c in range(ncol):
            vals = [r[c + 1] for r in chunk if r[c + 1] is not None]
            buckets[c].append(round(sum(vals) / len(vals), 2) if vals else None)
        i += step
    return {"ts": out_ts, "series": buckets}


def q_history(range_s, metric):
    cols = {
        "cpu":    "cpu, load_norm",
        "mem":    "mem_used, strata_rss, swap_used",
        "strata": "strata_rss",
        "gpu":    "gpu_util, gpu_power / NULLIF(gpu_plimit,0) * 100, gpu_power",
        "gputemp": "gpu_temp, gpu_temp_max",
        "gpumem": "gpu_mem",
        "tok":    "tok_s, prefill_tok_s",
        "net":    "net_rx, net_tx",
        "disk":   "disk_r, disk_w",
        "temp":   "gpu_temp, gpu_temp_max, cpu_temp",
        "nvme":   "nvme_temp",
        "psi":    "psi_cpu, psi_io",
    }
    col = cols.get(metric, cols["cpu"])
    with DB_LOCK:
        rows = DB.execute(
            f"SELECT ts, {col} FROM snap WHERE ts >= ? ORDER BY ts",
            (time.time() - range_s,)).fetchall()
    return downsample(rows)


def q_gpu_history(range_s):
    with DB_LOCK:
        rows = DB.execute(
            "SELECT ts,i,util,mem,temp,power,clk FROM gpub WHERE ts>=? ORDER BY ts,i",
            (time.time() - range_s,)).fetchall()
    by = {}
    for ts, i, util, mem, temp, power, clk in rows:
        by.setdefault(i, []).append((ts, util, mem, temp, power, clk))
    return {str(i): downsample(v) for i, v in by.items()}


def q_requests(limit=40):
    with DB_LOCK:
        rows = DB.execute(
            "SELECT id,ts,dur,prompt,reused,out,ttft,prompt_ms,decode_ms,decode_toks,hit,drafts,acc,finish "
            "FROM req ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    keys = ("id", "ts", "dur", "prompt", "reused", "out", "ttft", "prompt_ms",
            "decode_ms", "decode_toks", "hit", "drafts", "acc", "finish")
    return [dict(zip(keys, r)) for r in rows]


def q_events(limit=40):
    with DB_LOCK:
        rows = DB.execute("SELECT ts,key,level,text FROM evt ORDER BY ts DESC LIMIT ?",
                          (limit,)).fetchall()
    return [{"ts": r[0], "key": r[1], "level": r[2], "text": r[3]} for r in rows]


# ============================ 后台循环 ============================
CURRENT = {"ts": 0}
CUR_LOCK = threading.Lock()


def sampler():
    last_store = 0
    last_gpu_store = 0
    last_prune = 0
    while True:
        t0 = time.time()
        try:
            snap = collect()
            with CUR_LOCK:
                CURRENT.clear()
                CURRENT.update(snap)
            if snap["ts"] - last_store >= STORE_INTERVAL:
                store(snap)
                store_reqs(snap)
                last_store = snap["ts"]
            if snap["ts"] - last_gpu_store >= GPU_STORE_INTERVAL:
                store_gpu(snap)
                last_gpu_store = snap["ts"]
            if snap["ts"] - last_prune >= 3600:
                db_prune()
                last_prune = snap["ts"]
        except Exception as e:
            CURRENT["err"] = f"{type(e).__name__}: {e}"
        time.sleep(max(0.3, SAMPLE_INTERVAL - (time.time() - t0)))


# ============================ GPU 出力快采 ============================
# 只跑 nvidia-smi、只留变色需要的字段、只写内存不落库（落库仍按 GPU_STORE_INTERVAL）。
# 一次查询开销约几十 ms，0.7s 一周期的占空比在 10% 以内。
GPU_LIVE = {"ts": 0.0, "gpus": [], "interval": GPU_FAST_INTERVAL, "err": None}
GPU_LIVE_LOCK = threading.Lock()


def gpu_fast_loop():
    while True:
        t0 = time.time()
        try:
            gpus, err = jgpu()
            if gpus:
                slim = [{"i": x["i"], "util": x["util"], "power": x["power"],
                         "plimit": x["plimit"], "pmax": x["pmax"], "temp": x["temp"]}
                        for x in gpus]
                with GPU_LIVE_LOCK:
                    GPU_LIVE["ts"] = time.time()
                    GPU_LIVE["gpus"] = slim
                    GPU_LIVE["err"] = None
            elif err:
                with GPU_LIVE_LOCK:
                    GPU_LIVE["err"] = str(err)[:160]
        except Exception as e:
            with GPU_LIVE_LOCK:
                GPU_LIVE["err"] = f"{type(e).__name__}: {e}"[:160]
        time.sleep(max(0.05, GPU_FAST_INTERVAL - (time.time() - t0)))


def gpu_live_payload():
    with GPU_LIVE_LOCK:
        return dict(GPU_LIVE)


def live_payload():
    with CUR_LOCK:
        s = dict(CURRENT)
    # 精简 engine：去掉 history（前端不要），requests 只留 20 条
    eng = s.get("engine")
    if isinstance(eng, dict):
        eng = {k: v for k, v in eng.items() if k != "history"}
        eng["requests"] = (eng.get("requests") or [])[:20]
        s["engine"] = eng
    return s


def meta_payload():
    statics = {}
    txt = read_file("/proc/cpuinfo")
    for ln in txt.splitlines():
        if ln.startswith("model name"):
            statics["cpu_name"] = ln.split(":", 1)[1].strip()
            break
    statics["host"] = HOST
    statics["kernel"] = os.uname().release
    statics["nproc"] = os.cpu_count()
    statics["engine_url"] = ENGINE_URL
    statics["port"] = PORT
    return statics


# ============================ HTTP ============================
INDEX_HTML = BASE / "index.html"


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8", cache="no-store"):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def do_GET(self):
        u = urlparse(self.path)
        p = u.path
        q = parse_qs(u.query)
        try:
            if p == "/":
                html = INDEX_HTML.read_text(encoding="utf-8") if INDEX_HTML.exists() else "<h1>index.html 缺失</h1>"
                return self._send(200, html, "text/html; charset=utf-8")
            if p == "/api/live":
                return self._send(200, json.dumps(live_payload(), ensure_ascii=False))
            if p == "/api/gpu-live":
                return self._send(200, json.dumps(gpu_live_payload(), ensure_ascii=False))
            if p == "/api/meta":
                return self._send(200, json.dumps(meta_payload(), ensure_ascii=False))
            if p == "/api/history":
                rng = float(q.get("range", ["3600"])[0])
                metric = q.get("metric", ["cpu"])[0]
                return self._send(200, json.dumps(q_history(rng, metric), ensure_ascii=False))
            if p == "/api/gpu-history":
                rng = float(q.get("range", ["3600"])[0])
                return self._send(200, json.dumps(q_gpu_history(rng), ensure_ascii=False))
            if p == "/api/requests":
                return self._send(200, json.dumps(
                    q_requests(int(q.get("limit", ["40"])[0])), ensure_ascii=False))
            if p == "/api/events":
                return self._send(200, json.dumps(q_events(int(q.get("limit", ["40"])[0])),
                                                  ensure_ascii=False))
            if p == "/health":
                return self._send(200, json.dumps({"ok": True, "ts": time.time()}))
            return self._send(404, json.dumps({"error": "not found"}))
        except BrokenPipeError:
            pass
        except Exception as e:
            return self._send(500, json.dumps({"error": f"{type(e).__name__}: {e}"}))


def main():
    print(f"[hw-dashboard] {HOST} http://0.0.0.0:{PORT}  采样 {SAMPLE_INTERVAL}s  落库 {STORE_INTERVAL}s"
          + (f"  GPU 快采 {GPU_FAST_INTERVAL}s" if GPU_FAST_INTERVAL > 0 else "  GPU 快采已关"))
    threading.Thread(target=sampler, daemon=True).start()
    if GPU_FAST_INTERVAL > 0:
        threading.Thread(target=gpu_fast_loop, daemon=True).start()
    ThreadingHTTPServer.allow_reuse_address = True
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()


if __name__ == "__main__":
    main()
