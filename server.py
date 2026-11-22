#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""算法可视化演示器（纯演示层 / 前端）

设计原则
--------
1. 只做“展示”：本文件不读取、不修改、不注入任何算法源码。
2. 想演示哪个算法，就把那个 .py 放在本文件同级目录，页面里选中即可。
3. 运行方式：以子进程启动 `python -u 你的算法.py`，逐行读取它的 stdout，
   把每一行里的 “[1, 2, 3]” 解析成数组帧，用 SSE 实时推给浏览器画成柱状图。
4. “演示速度”只在子进程运行时临时接管 time.sleep，用于加速/放慢动画，
   .py 文件本身一个字节都不会被改动。

零第三方依赖，只用标准库。启动：

    python server.py                # 默认 http://127.0.0.1:8000
    python server.py --port 8080 --no-browser
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
from urllib.parse import parse_qs, unquote, urlparse

# --------------------------------------------------------------------------
# 基础配置
# --------------------------------------------------------------------------

FROZEN = bool(getattr(sys, "frozen", False))
# 打包成 exe 后 __file__ 指向临时解包目录，算法文件要相对 exe 所在目录找
BASE_DIR = os.path.dirname(os.path.abspath(sys.executable if FROZEN else __file__))
SELF_NAME = os.path.basename(os.path.abspath(sys.executable if FROZEN else __file__))
# 打包后 exe 本身就是本文件，演示列表里要把它排除掉
_SELF_NAMES = {SELF_NAME, "server.py"}

MAX_RUNTIME = 300.0        # 单个算法进程最长运行时间（秒）
MAX_ELEMENTS = 2000        # 单行数组最多解析多少个元素
MAX_LINE_BYTES = 20000     # 单行数组文本最大长度
_READY_MARK = "__ALGO_START__"   # 子进程真正开始跑算法时打的标记（用于精确计时）

# 运行中的子进程登记表：token -> Popen
_RUNS: "dict[str, subprocess.Popen]" = {}
_RUNS_LOCK = threading.Lock()

# 以 -c 方式注入的极薄外壳：只调整 time.sleep 的实参，不触碰算法源码。
_SLEEP_SHIM = (
    "import runpy, sys, time\n"
    "path, factor = sys.argv[1], float(sys.argv[2])\n"
    "_sleep = time.sleep\n"
    "time.sleep = lambda s: _sleep(max(0.0, float(s) / factor))\n"
    "sys.argv = [path]\n"
    "runpy.run_path(path, run_name='__main__')\n"
)

# --------------------------------------------------------------------------
# 算法文件发现 / 校验（只做白名单校验，不解析内容）
# --------------------------------------------------------------------------


def list_algorithms():
    """列出同级目录下可演示的 .py 文件（排除本文件与下划线开头的辅助文件）。"""
    items = []
    try:
        names = os.listdir(BASE_DIR)
    except OSError:
        return items
    for name in sorted(names, key=lambda s: s.lower()):
        if not name.lower().endswith(".py"):
            continue
        if name in _SELF_NAMES or name.startswith("_") or name.startswith("."):
            continue
        path = os.path.join(BASE_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            stat = os.stat(path)
        except OSError:
            continue
        items.append(
            {
                "name": name,
                "size": stat.st_size,
                "mtime": int(stat.st_mtime),
            }
        )
    return items


def resolve_algorithm(raw_name):
    """把请求里的文件名解析成安全的绝对路径；不允许越界、不允许演示自身。"""
    name = os.path.basename((raw_name or "").strip().replace("\\", "/").split("/")[-1])
    if not name:
        raise ValueError("请选择一个算法文件")
    if not name.lower().endswith(".py"):
        raise ValueError("只能演示 .py 文件")
    if name in _SELF_NAMES:
        raise ValueError("不能演示可视化服务自身")
    if name.startswith(".") or name.startswith("_"):
        raise ValueError("该文件被排除在演示列表之外")
    path = os.path.abspath(os.path.join(BASE_DIR, name))
    if os.path.dirname(path) != BASE_DIR:
        raise ValueError("非法的文件路径")
    if not os.path.isfile(path):
        raise ValueError("文件不存在：%s" % name)
    return path


# --------------------------------------------------------------------------
# 子进程 stdout -> 帧 解析
# --------------------------------------------------------------------------

_NUM_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_LIST_RE = re.compile(r"\[([^\[\]]{0,%d})\]" % MAX_LINE_BYTES)


def _parse_number(token):
    token = token.strip()
    if not _NUM_RE.match(token):
        raise ValueError(token)
    if "." in token or "e" in token or "E" in token:
        return float(token)
    return int(token)


_SCALAR_RE = re.compile(
    r"([A-Za-z_\u4e00-\u9fff][A-Za-z0-9_\u4e00-\u9fff ]{0,15}?)\s*[:：=]\s*"
    r"([+-]?(?:\d+(?:\.\d*)?|\.\d+))(?![\d.])"
)
_LABEL_SPLIT_RE = re.compile(r"[,，;；|]")
_CAPTION_TRIM = " \t:：,，.。;；=→>|-"
# “取出/待插入”这类标签，用来识别被抽出数组的那个元素
_BARE_NUM_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_HELD_RE = re.compile(r"取出|拿出|抽出|当前|待插入|插入值|临时|key|value|temp|hold|picked", re.I)


def _clean_text(text):
    return text.strip().strip(_CAPTION_TRIM).strip()


def _consume_gap(gap):
    """解析两个数组之间的文字：拆出 `pivot: 5` 这类标量，剩下的当数组标签。"""
    scalars = []
    rest = gap
    for match in _SCALAR_RE.finditer(gap):
        label = _clean_text(match.group(1))
        if not label:
            continue
        scalars.append({"label": label, "value": _parse_number(match.group(2))})
        rest = rest.replace(match.group(0), " ", 1)
    pieces = [piece for piece in _LABEL_SPLIT_RE.split(rest) if piece.strip()]
    label = _clean_text(pieces[-1]) if pieces else ""
    free = [_clean_text(piece) for piece in pieces[:-1]]
    free = [piece for piece in free if piece]
    return scalars, label, " ".join(free)


def _try_parse_array(match):
    body = match.group(1).strip()
    if not body:
        return []
    parts = body.split(",")
    if len(parts) > MAX_ELEMENTS:
        return None
    try:
        return [_parse_number(part) for part in parts]
    except ValueError:
        return None


def parse_frame_line(line):
    """把一行输出解析成一帧。

    支持同一行里出现多个数组，例如快速排序的
    `left: [1, 2] pivot: 5 right: [6, 7]`
    -> arrays=[{label:"left",...},{label:"right",...}], scalars=[{label:"pivot",value:5}]

    只有一个数组时，数组前面的文字作为 caption：
    `升序排序后的数组是: [3, 7, 12]` -> caption="升序排序后的数组是"

    不含数字数组的行 -> None
    """
    text = line.replace("\r", "").strip()
    if not text:
        return None

    # 整行就是一个数字：插入排序里 print(key) 打印“被取出的元素”
    if _BARE_NUM_RE.match(text):
        return {
            "arrays": [],
            "scalars": [{"label": "取出元素", "value": _parse_number(text)}],
            "caption": "",
            "values": [],
        }

    arrays = []
    scalars = []
    leftover = []
    prev_end = 0
    first_gap = None

    for match in _LIST_RE.finditer(text):
        gap = text[prev_end:match.start()]
        values = _try_parse_array(match)
        if values is None:
            leftover.append(gap + match.group(0))
            prev_end = match.end()
            continue
        if first_gap is None:
            first_gap = gap
        else:
            gap_scalars, label, free = _consume_gap(gap)
            scalars.extend(gap_scalars)
            arrays.append({"label": label, "values": values})
            if free:
                leftover.append(free)
            prev_end = match.end()
            continue
        # 第一个数组：先停下，等确定这行到底有几个数组再决定 gap 的用途
        arrays.append({"label": "", "values": values})
        prev_end = match.end()

    tail = text[prev_end:]
    if not arrays:
        # 只打印了“取出元素: 16”这种没带数组的行：交给前端把那根柱子抬起来
        loose = []
        for match in _SCALAR_RE.finditer(text):
            label = _clean_text(match.group(1))
            if label:
                loose.append({"label": label, "value": _parse_number(match.group(2))})
        if loose and any(_HELD_RE.search(item["label"]) for item in loose):
            caption = _clean_text(_SCALAR_RE.sub(" ", text))
            return {"arrays": [], "scalars": loose, "caption": caption, "values": []}
        return None

    if len(arrays) == 1:
        # 单数组行：先把 “间隔: 5” 这类标量抠出来，剩下的文字才当字幕
        arrays[0]["label"] = ""
        gap_scalars, _label, _free = _consume_gap(first_gap or "")
        if gap_scalars:
            scalars.extend(gap_scalars)
            rest = first_gap or ""
            for match in _SCALAR_RE.finditer(rest):
                rest = rest.replace(match.group(0), " ", 1)
            caption = _clean_text(_LABEL_SPLIT_RE.sub(" ", rest) + " " + tail)
        else:
            caption = _clean_text((first_gap or "") + " " + tail)
    else:
        caption = _clean_text(" ".join(leftover + [tail]))
        if first_gap:
            gap_scalars, label, free = _consume_gap(first_gap)
            scalars[:0] = gap_scalars
            arrays[0]["label"] = label
            if free:
                caption = _clean_text(free + " " + caption)

    return {
        "arrays": arrays,
        "scalars": scalars,
        "caption": caption,
        "values": arrays[0]["values"],
    }


# --------------------------------------------------------------------------
# 运行子进程并逐块产出事件
# --------------------------------------------------------------------------


def _child_env():
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    return env


def build_command(path, speed):
    """返回 (argv, 给人看的命令说明)。命令说明只用于前端展示。

    普通运行时用当前 Python 解释器；打包成 exe 后没有 python.exe 可用，
    所以用 exe 自己的 --run 模式把脚本跑在子进程里（仍是独立进程）。
    """
    script = os.path.basename(path)
    if FROZEN:
        argv = [sys.executable, "--run", path, repr(float(speed))]
        label = "%s --run %s%s" % (os.path.basename(sys.executable), script,
                                   "" if abs(speed - 1.0) < 1e-9 else " (×%g)" % speed)
    elif abs(speed - 1.0) > 1e-9:
        argv = [sys.executable, "-u", "-c", _SLEEP_SHIM, path, repr(speed)]
        label = "python -u -c <sleep-shim ×%g> %s" % (speed, script)
    else:
        argv = [sys.executable, "-u", path]
        label = "python -u %s" % script
    return argv, label


def run_script(path, speed):
    """--run 模式：把自己当成解释器执行用户脚本（打包后没有 python.exe 时用）。"""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    if abs(speed - 1.0) > 1e-9:
        import time as _time
        _sleep = _time.sleep
        _time.sleep = lambda sec: _sleep(max(0.0, float(sec) / speed))
    sys.argv = [path]
    # 告诉父进程“算法从这里开始跑”，打包成 exe 后的启动开销不算进运行时间
    print(_READY_MARK, flush=True)
    import runpy
    runpy.run_path(path, run_name="__main__")


def _register(token, proc):
    if token:
        with _RUNS_LOCK:
            _RUNS[token] = proc


def _unregister(token):
    if token:
        with _RUNS_LOCK:
            _RUNS.pop(token, None)


def stop_run(token):
    with _RUNS_LOCK:
        proc = _RUNS.get(token)
    if proc is None:
        return False
    _terminate(proc)
    return True


def _terminate(proc):
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass


def iter_process(path, speed, token):
    """产出 (kind, payload)：
    ('out', line) / ('err', line) 子进程的一行输出
    ('ping', None)                心跳（长时间无输出时）
    ('timeout', None)             超出 MAX_RUNTIME
    ('exit', code)                进程退出码
    """
    cmd, _label = build_command(path, speed)
    proc = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=_child_env(),
    )
    _register(token, proc)

    queue: "Queue" = Queue()

    def reader(stream, tag):
        try:
            for raw in stream:
                queue.put((tag, raw.rstrip("\r\n")))
        except Exception:
            pass
        finally:
            queue.put((tag, None))

    threads = [
        threading.Thread(target=reader, args=(proc.stdout, "out"), daemon=True),
        threading.Thread(target=reader, args=(proc.stderr, "err"), daemon=True),
    ]
    for thread in threads:
        thread.start()

    started = time.monotonic()
    closed = 0
    timed_out = False
    try:
        while closed < 2:
            try:
                tag, line = queue.get(timeout=1.5)
            except Empty:
                if time.monotonic() - started > MAX_RUNTIME:
                    timed_out = True
                    _terminate(proc)
                    yield ("timeout", None)
                    break
                yield ("ping", None)
                continue
            if line is None:
                closed += 1
                continue
            if tag == "out" and line.strip() == _READY_MARK:
                yield ("ready", None)
                continue
            yield (tag, line)

        if not timed_out:
            try:
                code = proc.wait(timeout=5)
            except Exception:
                code = proc.poll()
            yield ("exit", code)
    finally:
        _unregister(token)
        _terminate(proc)
        for stream in (proc.stdout, proc.stderr):
            try:
                if stream:
                    stream.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# HTTP 处理
# --------------------------------------------------------------------------


class VizServer(ThreadingHTTPServer):
    """Windows 上必须关掉 SO_REUSEADDR，否则旧进程还活着时新进程也能“绑定成功”，
    结果端口被旧实例继续占用，改了代码却看不到效果。"""

    daemon_threads = True
    allow_reuse_address = os.name != "nt"

    def handle_error(self, request, client_address):
        # 浏览器关标签页/刷新时连接会被强行断开，这种噪音不必打印堆栈
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "AlgoViz/1.0"

    # 静默默认访问日志，只在出错时输出
    def log_message(self, fmt, *args):  # noqa: A003
        pass

    # ---------- 工具 ----------

    def send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def send_html(self, html):
        body = html.encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    # ---------- 路由 ----------

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path
        query = parse_qs(parsed.query)

        if route in ("/", "/index.html"):
            self.send_html(INDEX_HTML)
            return
        if route == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if route == "/api/algorithms":
            self.send_json({"files": list_algorithms(), "dir": BASE_DIR})
            return
        if route == "/api/stop":
            token = (query.get("token") or [""])[0]
            self.send_json({"ok": stop_run(token)})
            return
        if route == "/api/run":
            self.handle_run(query)
            return
        self.send_json({"error": "not found"}, 404)

    # ---------- 核心：SSE 推流 ----------

    def handle_run(self, query):
        raw_file = (query.get("file") or [""])[0]
        raw_speed = (query.get("speed") or ["1"])[0]
        token = (query.get("token") or [""])[0]

        try:
            path = resolve_algorithm(unquote(raw_file))
        except ValueError as exc:
            self.send_json({"error": str(exc)}, 400)
            return

        try:
            speed = float(raw_speed)
        except ValueError:
            speed = 1.0
        speed = max(0.25, min(32.0, speed))

        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("Connection", "close")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return

        def sse(event, data):
            chunk = ("event: %s\ndata: %s\n\n" % (event, json.dumps(data, ensure_ascii=False))).encode("utf-8")
            self.wfile.write(chunk)
            self.wfile.flush()

        started = time.monotonic()
        frame_no = 0
        exit_code = None
        timed_out = False
        gen = None

        try:
            sse(
                "meta",
                {
                    "file": os.path.basename(path),
                    "speed": speed,
                    "token": token,
                    "command": build_command(path, speed)[1],
                    "max_runtime": MAX_RUNTIME,
                },
            )
            gen = iter_process(path, speed, token)
            for kind, payload in gen:
                if kind == "ready":
                    started = time.monotonic()
                    continue
                if kind == "ping":
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                if kind == "out":
                    sse("log", {"text": payload, "stream": "stdout"})
                    frame = parse_frame_line(payload)
                    if frame is not None:
                        frame_no += 1
                        sse(
                            "frame",
                            {
                                "i": frame_no,
                                "t": round(time.monotonic() - started, 3),
                                "arrays": frame["arrays"],
                                "scalars": frame["scalars"],
                                "values": frame["values"],
                                "caption": frame["caption"],
                            },
                        )
                    continue
                if kind == "err":
                    sse("log", {"text": payload, "stream": "stderr"})
                    continue
                if kind == "timeout":
                    timed_out = True
                    sse(
                        "log",
                        {
                            "text": "运行超过 %.0f 秒，已强制结束进程。" % MAX_RUNTIME,
                            "stream": "stderr",
                        },
                    )
                    continue
                if kind == "exit":
                    exit_code = payload
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # 浏览器关掉了连接：后台 finally 会终止子进程
            return
        except Exception as exc:  # 兜底，避免把异常写进 SSE 流
            try:
                sse("failed", {"message": "%s: %s" % (type(exc).__name__, exc)})
            except OSError:
                return
            return
        finally:
            if gen is not None:
                try:
                    gen.close()
                except Exception:
                    pass

        try:
            sse(
                "done",
                {
                    "frames": frame_no,
                    "code": exit_code,
                    "elapsed": round(time.monotonic() - started, 2),
                    "timedOut": timed_out,
                    "ok": (exit_code == 0) and not timed_out,
                },
            )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass


# --------------------------------------------------------------------------
# 前端页面（HTML + CSS + JS 一体，免安装、免构建）
# --------------------------------------------------------------------------

INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>算法可视化演示器</title>
<style>
:root{
  --bg:#070b16; --card:rgba(17,25,45,.72); --card2:rgba(13,20,38,.6);
  --line:rgba(110,150,255,.16); --txt:#e8eefc; --dim:#8ea0c4;
  --bar-a:#38bdf8; --bar-b:#6366f1; --hot-a:#fde68a; --hot-b:#f59e0b;
  --ok-a:#6ee7b7; --ok-b:#10b981; --err:#fb7185; --accent:#5eead4;
  --r:18px;
}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%}
body{
  font-family:"Segoe UI","PingFang SC","Microsoft YaHei",system-ui,-apple-system,sans-serif;
  color:var(--txt);
  background:
    radial-gradient(1100px 560px at 10% -12%,rgba(56,189,248,.16),transparent 62%),
    radial-gradient(900px 520px at 102% -4%,rgba(129,140,248,.20),transparent 58%),
    radial-gradient(900px 700px at 48% 118%,rgba(52,211,153,.10),transparent 62%),
    var(--bg);
  background-attachment:fixed;
}
body::before{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:0;
  background-image:linear-gradient(rgba(120,160,255,.055) 1px,transparent 1px),
                   linear-gradient(90deg,rgba(120,160,255,.055) 1px,transparent 1px);
  background-size:56px 56px;
  mask-image:radial-gradient(circle at 50% 20%,#000 0%,transparent 78%);
  -webkit-mask-image:radial-gradient(circle at 50% 20%,#000 0%,transparent 78%);
}
.wrap{
  position:relative;z-index:1;max-width:1420px;margin:0 auto;padding:22px 26px 34px;
  min-height:100vh;display:flex;flex-direction:column;
}

/* ---------- 顶栏 ---------- */
.topbar{
  display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;
  gap:18px;padding:16px 20px;margin-bottom:18px;
  background:linear-gradient(180deg,rgba(23,33,58,.82),rgba(14,21,39,.72));
  border:1px solid var(--line);border-radius:calc(var(--r) + 4px);
  box-shadow:0 20px 50px -28px rgba(0,0,0,.9),inset 0 1px 0 rgba(255,255,255,.05);
  backdrop-filter:blur(14px);
}
.brand{display:flex;align-items:center;gap:14px}
.logo{
  width:46px;height:46px;flex:0 0 46px;display:grid;place-items:center;
  border-radius:14px;font-size:22px;color:#06111f;
  background:linear-gradient(140deg,#5eead4,#60a5fa 55%,#a78bfa);
  box-shadow:0 10px 26px -8px rgba(96,165,250,.75),inset 0 1px 0 rgba(255,255,255,.6);
}
.brand h1{margin:0;font-size:19px;letter-spacing:.4px;font-weight:700}
.brand p{margin:3px 0 0;font-size:12.5px;color:var(--dim)}
.brand code,.foot code{
  font-family:ui-monospace,SFMono-Regular,Consolas,"Courier New",monospace;
  background:rgba(120,160,255,.12);border:1px solid rgba(120,160,255,.18);
  padding:1px 6px;border-radius:6px;font-size:11.5px;color:#c7d6ff;
}
.controls{display:flex;flex-wrap:wrap;align-items:flex-end;gap:12px}
.field{display:flex;flex-direction:column;gap:6px}
.field>span{font-size:11px;letter-spacing:.9px;color:var(--dim);text-transform:uppercase}
select{
  appearance:none;min-width:150px;padding:9px 32px 9px 12px;border-radius:11px;
  color:var(--txt);font-size:13.5px;font-family:inherit;cursor:pointer;
  background:rgba(9,15,30,.9) url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'><path d='M2 4l4 4 4-4' fill='none' stroke='%238ea0c4' stroke-width='1.6' stroke-linecap='round'/></svg>") no-repeat right 11px center;
  border:1px solid rgba(120,160,255,.22);
  transition:border-color .2s,box-shadow .2s;
}
select:hover{border-color:rgba(120,160,255,.42)}
select:focus{outline:none;border-color:rgba(94,234,212,.6);box-shadow:0 0 0 3px rgba(94,234,212,.14)}
.btnrow{display:flex;gap:9px;align-items:center}
.btn{
  font-family:inherit;font-size:13.5px;font-weight:600;letter-spacing:.3px;
  padding:10px 17px;border-radius:11px;cursor:pointer;color:var(--txt);
  background:rgba(120,160,255,.10);border:1px solid rgba(120,160,255,.24);
  transition:transform .12s,background .2s,box-shadow .2s,border-color .2s;
  white-space:nowrap;
}
.btn:hover:not(:disabled){background:rgba(120,160,255,.2);border-color:rgba(120,160,255,.44)}
.btn:active:not(:disabled){transform:translateY(1px)}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn.primary{
  color:#04121f;border:none;
  background:linear-gradient(135deg,#5eead4,#38bdf8 58%,#818cf8);
  box-shadow:0 12px 26px -12px rgba(56,189,248,.9),inset 0 1px 0 rgba(255,255,255,.5);
}
.btn.primary:hover:not(:disabled){box-shadow:0 16px 32px -12px rgba(56,189,248,1);filter:brightness(1.06)}
.btn.ghost{padding:10px 13px;font-size:15px;line-height:1}

/* ---------- 布局 ---------- */
.grid{display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:18px;align-items:stretch;flex:1 1 auto}
.card{
  background:var(--card);border:1px solid var(--line);border-radius:var(--r);
  box-shadow:0 24px 60px -34px rgba(0,0,0,.95),inset 0 1px 0 rgba(255,255,255,.045);
  backdrop-filter:blur(12px);
}

/* ---------- 舞台 ---------- */
.stage-card{padding:18px 20px 14px;display:flex;flex-direction:column;min-height:520px}
.stage-head{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:6px}
.caption-wrap{display:flex;align-items:center;gap:12px;min-width:0}
.badge{
  font-size:11.5px;font-weight:700;letter-spacing:.6px;padding:6px 11px;border-radius:999px;
  border:1px solid transparent;white-space:nowrap;
}
.badge.idle{color:#9fb2d4;background:rgba(120,160,255,.10);border-color:rgba(120,160,255,.22)}
.badge.running{color:#7dd3fc;background:rgba(56,189,248,.14);border-color:rgba(56,189,248,.4)}
.badge.running::before{
  content:"";display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:7px;
  background:#38bdf8;vertical-align:1px;animation:pulse 1.05s ease-in-out infinite;
}
.badge.done{color:#6ee7b7;background:rgba(16,185,129,.16);border-color:rgba(16,185,129,.45)}
.badge.error{color:#fda4af;background:rgba(251,113,133,.14);border-color:rgba(251,113,133,.4)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.35;transform:scale(.75)}}
#caption{margin:0;font-size:16px;font-weight:600;letter-spacing:.2px;color:#dbe6ff;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:56vw}
.chips{display:flex;gap:8px;flex-wrap:wrap}
.chip{
  font-size:12px;color:var(--dim);padding:6px 11px;border-radius:999px;
  background:var(--card2);border:1px solid var(--line);font-variant-numeric:tabular-nums;
}
.chips-scalars{display:contents}
.chip.scalar{color:#c4b5fd;border-color:rgba(167,139,250,.35);background:rgba(167,139,250,.12)}
.chip.scalar b{color:#e9e2ff;font-weight:700}

.stage{
  position:relative;flex:1 1 auto;min-height:360px;margin:8px 0 6px;padding:10px 8px 4px;
  display:flex;flex-direction:column;
  border-radius:14px;overflow:hidden;
  background:
    linear-gradient(180deg,rgba(8,13,26,.55),rgba(8,13,26,.18)),
    repeating-linear-gradient(180deg,rgba(120,160,255,.07) 0 1px,transparent 1px 25%);
  border:1px solid rgba(120,160,255,.10);
}
.groups{display:flex;align-items:stretch;gap:16px;flex:1 1 auto;min-height:0;padding:2px}
.group{flex:1 1 0;min-width:0;min-height:0;display:flex;flex-direction:column}
.group-label{
  display:flex;align-items:center;justify-content:center;gap:7px;
  height:24px;flex:0 0 24px;font-size:11.5px;font-weight:700;letter-spacing:.6px;
  color:#9fb6e0;text-transform:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.group-label::before{
  content:"";width:6px;height:6px;border-radius:2px;flex:0 0 6px;
  background:linear-gradient(180deg,var(--bar-a),var(--bar-b));opacity:.85;
}
.group-label:empty{display:none}
.group-label .cnt{color:rgba(142,160,196,.7);font-weight:600}
.group-empty{
  flex:1;display:grid;place-items:center;font-size:12px;color:rgba(142,160,196,.55);
  border:1px dashed rgba(120,160,255,.16);border-radius:12px;margin:6px 2px;
}
.bars{display:flex;align-items:stretch;justify-content:center;gap:clamp(4px,1vw,14px);flex:1 1 0;min-height:0;padding:6px 4px 0;position:relative}

/* ---------- 结构标注层：间隔分组 / 桶边界 / 堆 ---------- */
.overlay{position:absolute;inset:0;pointer-events:none}
.bars.gapped .col:not(.changed) .bar{
  background:linear-gradient(180deg,var(--g1,var(--bar-a)),var(--g2,var(--bar-b)));
  box-shadow:0 10px 24px -18px rgba(0,0,0,.9),inset 0 1px 0 rgba(255,255,255,.28);
}
.bucket-band{
  position:absolute;top:0;bottom:0;border-radius:8px;display:block;
  background:rgba(120,160,255,.05);
}
.bucket-band.odd{background:rgba(120,160,255,.11)}
.bucket-div{position:absolute;top:0;bottom:0;border-left:1px dashed rgba(130,170,255,.45)}
.bucket-tag{
  position:absolute;bottom:22px;padding:1px 5px;border-radius:6px;font-size:9.5px;
  background:rgba(9,15,30,.78);border:1px solid rgba(120,160,255,.25);
  color:rgba(170,190,225,.92);white-space:nowrap;
}
.heap-div{position:absolute;top:0;bottom:0;border-left:1px dashed rgba(52,211,153,.55)}
.heap-tag{
  position:absolute;bottom:22px;padding:1px 5px;border-radius:6px;font-size:9.5px;
  background:rgba(6,32,26,.85);border:1px solid rgba(52,211,153,.35);
  color:rgba(110,231,183,.95);white-space:nowrap;
}
.col.tail .bar{opacity:.32;filter:saturate(.25)}
.col.tail .col-val{color:rgba(142,160,196,.38)}
.col.cur .bar{outline:1px dashed rgba(251,191,36,.85);outline-offset:2px}
.tree-lines{position:absolute;left:0;top:0;width:100%;height:30px;overflow:visible}
.tree-lines path{fill:none;stroke:rgba(130,175,255,.5);stroke-width:1.1}

/* ---------- 递归分区树（一行多个数组时启用） ---------- */
.tree{display:none;flex:1 1 auto;min-height:0;overflow:auto;padding:6px 4px 8px;position:relative;flex-direction:column}
.tree.on{display:flex}
.groups.off{display:none}
.tree-inner{position:relative;width:100%;margin:auto 0}
.node{
  position:absolute;height:90px;display:flex;align-items:stretch;overflow:hidden;
  border:1px dashed rgba(120,160,255,.30);border-radius:10px;
  background:linear-gradient(180deg,rgba(15,23,44,.72),rgba(11,17,33,.5));
  transition:border-color .3s,box-shadow .3s,opacity .3s;
}
.node.idle{opacity:.5}
.node.active{border-color:rgba(94,234,212,.42);box-shadow:inset 0 0 0 1px rgba(94,234,212,.10)}
.node.fresh{
  border-color:rgba(251,191,36,.75);
  box-shadow:0 0 24px -6px rgba(251,191,36,.55),inset 0 0 0 1px rgba(251,191,36,.18);
}
.node.result{border-color:rgba(52,211,153,.6);background:linear-gradient(180deg,rgba(8,45,34,.55),rgba(8,32,26,.35))}
.seg{flex:1 1 0;min-width:0;display:flex;flex-direction:column;padding:2px 3px 0}
.seg + .seg{border-left:1px dashed rgba(130,170,255,.5)}
.seg-bars{flex:1 1 0;min-height:0;display:flex;align-items:flex-end;justify-content:center;gap:2px}
.tbar{
  flex:1 1 0;min-width:0;max-width:70px;display:flex;flex-direction:column;
  justify-content:flex-end;align-items:center;
}
.tval{
  height:13px;line-height:13px;font-size:9.5px;color:rgba(203,218,246,.82);
  font-variant-numeric:tabular-nums;white-space:nowrap;
}
.tfill{
  width:100%;border-radius:4px 4px 2px 2px;
  background:linear-gradient(180deg,var(--bar-a),var(--bar-b));
  box-shadow:inset 0 1px 0 rgba(255,255,255,.25);
}
.seg-mid .tfill{
  background:linear-gradient(180deg,var(--hot-a),var(--hot-b));
  box-shadow:0 0 16px -5px rgba(251,191,36,.85),inset 0 1px 0 rgba(255,255,255,.5);
}
.node.result .tfill{background:linear-gradient(180deg,var(--ok-a),var(--ok-b));box-shadow:none}
.seg-name{
  height:14px;line-height:14px;font-size:9.5px;text-align:center;white-space:nowrap;
  overflow:hidden;color:rgba(142,160,196,.62);
}
.seg.dense .tval{display:none}

.col{
  flex:1 1 0;min-width:0;display:flex;flex-direction:column;
  justify-content:flex-end;align-items:center;position:relative;
}
/* 数值标签 + 柱子绑在一起，交换时整体平移 */
.stack{
  width:100%;display:flex;flex-direction:column;justify-content:flex-end;
  align-items:center;
}
.col.moving,.col.held{z-index:5}
.col.held .bar{box-shadow:0 26px 44px -14px rgba(94,234,212,.9),0 0 0 1px rgba(94,234,212,.6),inset 0 1px 0 rgba(255,255,255,.5)}
.col.moving .bar{box-shadow:0 26px 44px -14px rgba(94,234,212,.9),0 0 0 1px rgba(94,234,212,.6),inset 0 1px 0 rgba(255,255,255,.5)}
.col.moving .col-val{color:#5eead4}
.col.swapping{z-index:3}
.col.swapping.over{z-index:4}
.col.swapping .stack{will-change:transform}
.col.swapping .bar{
  box-shadow:0 22px 40px -16px rgba(251,191,36,.85),0 0 0 1px rgba(251,191,36,.55),
             inset 0 1px 0 rgba(255,255,255,.5);
}
.col-val{
  height:22px;line-height:22px;font-size:12.5px;font-weight:600;color:var(--dim);
  font-variant-numeric:tabular-nums;transition:color .22s,transform .22s;
}
.bar{
  width:100%;max-width:96px;height:0;
  border-radius:11px 11px 5px 5px;
  background:linear-gradient(180deg,var(--bar-a),var(--bar-b));
  box-shadow:0 12px 26px -16px rgba(56,189,248,.9),inset 0 1px 0 rgba(255,255,255,.32);
  transition:height .4s cubic-bezier(.22,1,.36,1),background .28s,box-shadow .28s,transform .28s;
}
.col-idx{
  height:24px;line-height:24px;font-size:10.5px;color:rgba(142,160,196,.5);
  font-variant-numeric:tabular-nums;
}
.bars.dense .col-val{font-size:9.5px;height:16px;line-height:16px}
.bars.dense .col-idx{display:none}
.col.changed .bar{
  background:linear-gradient(180deg,var(--hot-a),var(--hot-b));
  box-shadow:0 0 0 1px rgba(251,191,36,.55),0 0 30px rgba(251,191,36,.55),inset 0 1px 0 rgba(255,255,255,.6);
  transform:translateY(-5px);
}
.col.changed .col-val{color:#fcd34d;transform:translateY(-2px)}
.bars.sorted .bar{
  background:linear-gradient(180deg,var(--ok-a),var(--ok-b));
  box-shadow:0 12px 26px -16px rgba(16,185,129,.9),inset 0 1px 0 rgba(255,255,255,.35);
}
.bars.sorted .col.changed .bar{background:linear-gradient(180deg,var(--ok-a),var(--ok-b));transform:none}

.empty{
  position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:6px;text-align:center;padding:24px;transition:opacity .35s;pointer-events:none;
}
.empty.hide{opacity:0}
.empty-icon{font-size:34px;color:rgba(120,160,255,.35);letter-spacing:6px}
.empty p{margin:0;font-size:14px;color:#a9bade}
.empty small{font-size:12px;color:rgba(142,160,196,.72);max-width:380px;line-height:1.6}

.stage-foot{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap}
.legend{display:flex;gap:16px;flex-wrap:wrap}
.legend span{display:flex;align-items:center;gap:6px;font-size:11.5px;color:var(--dim)}
.dot{width:9px;height:9px;border-radius:3px;display:inline-block}
.bar-dot{background:linear-gradient(180deg,var(--bar-a),var(--bar-b))}
.hot-dot{background:linear-gradient(180deg,var(--hot-a),var(--hot-b))}
.ok-dot{background:linear-gradient(180deg,var(--ok-a),var(--ok-b))}
.hint{font-size:11.5px;color:rgba(142,160,196,.72);font-variant-numeric:tabular-nums}

/* ---------- 侧栏 ---------- */
.side{display:flex;flex-direction:column;gap:18px}
.side h3{margin:0;font-size:12px;letter-spacing:1.1px;color:var(--dim);text-transform:uppercase;font-weight:700}
.stats{padding:16px 18px}
.stats dl{margin:12px 0 0;display:flex;flex-direction:column;gap:0}
.stats dl>div{
  display:flex;align-items:baseline;justify-content:space-between;gap:12px;
  padding:9px 0;border-bottom:1px dashed rgba(120,160,255,.12);
}
.stats dl>div:last-child{border-bottom:none;padding-bottom:0}
.stats dt{font-size:12.5px;color:var(--dim);white-space:nowrap}
.stats dd{
  margin:0;font-size:12.8px;font-weight:600;color:#dbe6ff;text-align:right;
  font-variant-numeric:tabular-nums;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
}
.console{padding:14px 16px 12px;display:flex;flex-direction:column;min-height:0}
.console-head{display:flex;align-items:center;justify-content:space-between;gap:10px}
.mini{
  font-family:inherit;font-size:11px;color:var(--dim);cursor:pointer;padding:4px 9px;
  border-radius:8px;background:rgba(120,160,255,.08);border:1px solid var(--line);
  transition:color .2s,background .2s;
}
.mini:hover{color:var(--txt);background:rgba(120,160,255,.18)}
#log{
  margin:11px 0 0;height:230px;overflow:auto;font-size:11.5px;line-height:1.72;
  font-family:ui-monospace,SFMono-Regular,Consolas,"Courier New",monospace;
  color:#93a7c9;white-space:pre-wrap;word-break:break-word;
}
#log .ln{display:block}
#log .ln.err{color:#fda4af}
#log .ln.sys{color:#5eead4}
#log .ln.frame{color:#7dd3fc}
#log .ln:empty{display:none}

.foot{margin-top:20px;text-align:center;font-size:11.5px;color:rgba(142,160,196,.72);line-height:1.9}
.foot p{margin:2px 0}
.foot .dim{color:rgba(142,160,196,.5)}

@media (max-width:1080px){
  .grid{grid-template-columns:minmax(0,1fr)}
  .side{flex-direction:row;flex-wrap:wrap}
  .side>*{flex:1 1 300px}
  #caption{max-width:100%}
}
@media (max-width:640px){
  .wrap{padding:14px 14px 26px}
  .topbar{padding:14px}
  .controls{width:100%}
  .field,.field select{width:100%}
  .btnrow{width:100%;justify-content:stretch}
  .btn{flex:1}
  .stage-card{min-height:440px;padding:14px}
  .stage{min-height:280px}
}
</style>
</head>
<body>
<div class="wrap">

  <header class="topbar">
    <div class="brand">
      <div class="logo">▦</div>
      <div>
        <h1>算法可视化演示器</h1>
        <p>直接运行你写好的 <code>.py</code> 算法文件 · 把每一行 print 变成动画</p>
      </div>
    </div>

    <div class="controls">
      <label class="field">
        <span>算法文件</span>
        <select id="algo"></select>
      </label>
      <label class="field">
        <span>演示速度</span>
        <select id="speed">
          <option value="0.5">0.5× 更慢</option>
          <option value="1" selected>1× 原速</option>
          <option value="2">2× 快</option>
          <option value="4">4× 更快</option>
          <option value="8">8× 飞快</option>
          <option value="16">16× 极速</option>
        </select>
      </label>
      <label class="field">
        <span>播放节奏</span>
        <select id="pace">
          <option value="1400">很慢</option>
          <option value="1000">慢</option>
          <option value="700" selected>正常</option>
          <option value="380">快</option>
          <option value="160">极快</option>
        </select>
      </label>
      <div class="btnrow">
        <button id="run" class="btn primary">▶ 开始演示</button>
        <button id="stop" class="btn" disabled>■ 停止</button>
        <button id="refresh" class="btn ghost" title="重新扫描目录里的 .py 文件">⟳</button>
      </div>
    </div>
  </header>

  <main class="grid">
    <section class="card stage-card">
      <div class="stage-head">
        <div class="caption-wrap">
          <div id="status" class="badge idle">待运行</div>
          <h2 id="caption">选择算法文件，然后点击「开始演示」</h2>
        </div>
        <div class="chips">
          <span class="chip" id="chip-n">— 个元素</span>
          <span class="chip" id="chip-frame">第 0 帧</span>
          <span class="chips-scalars" id="scalars"></span>
        </div>
      </div>

      <div class="stage">
        <div class="groups" id="groups"></div>
        <div class="tree" id="tree"><div class="tree-inner" id="treeInner"></div></div>
        <div class="empty" id="empty">
          <div class="empty-icon">▥ ▥ ▥</div>
          <p>等待算法输出…</p>
          <small>服务端会以子进程运行所选 .py，逐行读取它的 stdout，并把每行里的数组解析成一帧柱状图（一行多个数组会自动分组显示）</small>
        </div>
      </div>

      <div class="stage-foot">
        <div class="legend">
          <span><i class="dot bar-dot"></i>当前数值</span>
          <span><i class="dot hot-dot"></i>刚刚变化</span>
          <span><i class="dot ok-dot"></i>排序完成</span>
        </div>
        <div class="hint" id="runinfo">未运行</div>
      </div>
    </section>

    <aside class="side">
      <section class="card stats">
        <h3>运行信息</h3>
        <dl>
          <div><dt>算法文件</dt><dd id="stat-file">—</dd></div>
          <div><dt>元素个数</dt><dd id="stat-n">—</dd></div>
          <div><dt>已渲染帧</dt><dd id="stat-frames">0</dd></div>
          <div><dt>变化帧数</dt><dd id="stat-swaps">0</dd></div>
          <div><dt>数值范围</dt><dd id="stat-range">—</dd></div>
          <div><dt>算法运行时间</dt><dd id="stat-time">—</dd></div>
        </dl>
      </section>

      <section class="card console">
        <div class="console-head">
          <h3>算法输出</h3>
          <button id="clear" class="mini">清空</button>
        </div>
        <pre id="log"></pre>
      </section>
    </aside>
  </main>

  <footer class="foot">
    <p>本演示层不读取、不修改、不注入算法源码：<code>server.py</code> 只是以子进程运行所选 <code>.py</code> 并接管它的 print 输出。</p>
    <p class="dim">「演示速度」仅在子进程运行时临时接管 <code>time.sleep</code> 的实参，磁盘上的算法文件保持原样。</p>
  </footer>
</div>

<script>
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var els = {
    algo: $("algo"), speed: $("speed"), pace: $("pace"),
    run: $("run"), stop: $("stop"), refresh: $("refresh"), clear: $("clear"),
    groups: $("groups"), tree: $("tree"), treeInner: $("treeInner"),
    empty: $("empty"), caption: $("caption"), status: $("status"),
    chipN: $("chip-n"), chipFrame: $("chip-frame"), runinfo: $("runinfo"),
    scalars: $("scalars"), log: $("log"),
    statFile: $("stat-file"), statN: $("stat-n"), statFrames: $("stat-frames"),
    statSwaps: $("stat-swaps"), statRange: $("stat-range"), statTime: $("stat-time")
  };

  var st = {
    es: null, token: null, running: false, done: false,
    queue: [], timer: null, clock: null,
    last: null, lastArrays: null, viewed: 0, changes: 0, frameSeq: 0,
    keepLast: false, exitCode: null, algTime: 0, held: null,
    tree: { active: false, root: null, stack: [], nodes: [], min: 0, max: 1, depth: 0, result: null }
  };

  var TREE_ROW_H = 98;      // 每一层分区占的高度
  var TREE_BAR_MAX = 62;    // 树里柱子的最大高度

  /* ---------------- 小工具 ---------------- */

  function fmt(v) {
    if (typeof v !== "number" || !isFinite(v)) return String(v);
    if (Number.isInteger(v)) return String(v);
    return String(Math.round(v * 100) / 100);
  }

  function isSortedAsc(a) {
    for (var i = 1; i < a.length; i++) if (a[i] < a[i - 1]) return false;
    return a.length > 1;
  }
  function isSortedDesc(a) {
    for (var i = 1; i < a.length; i++) if (a[i] > a[i - 1]) return false;
    return a.length > 1;
  }

  function setStatus(kind, text) {
    els.status.className = "badge " + kind;
    els.status.textContent = text;
  }

  function logLine(text, kind) {
    if (text === null || text === undefined) return;
    var nearBottom = els.log.scrollHeight - els.log.scrollTop - els.log.clientHeight < 60;
    var line = document.createElement("span");
    line.className = "ln " + (kind || "");
    line.textContent = text.length ? text : "";
    els.log.appendChild(line);
    while (els.log.childNodes.length > 500) els.log.removeChild(els.log.firstChild);
    if (nearBottom) els.log.scrollTop = els.log.scrollHeight;
  }

  function tickClock() {
    if (!st.running) return;
    // 统一为“算法进程的实际运行时间”：由服务端按帧上报（d.t），结束时用进程退出时的精确值
    els.statTime.textContent = st.algTime > 0 ? st.algTime.toFixed(2) + " s" : "—";
    els.runinfo.textContent = "运行中 · 已接收 " + st.frameSeq + " 帧 · 已渲染 " + st.viewed + " 帧";
  }

  /* ---------------- 算法列表 ---------------- */

  function loadAlgorithms() {
    var keep = els.algo.value;
    return fetch("/api/algorithms", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var files = (data && data.files) || [];
        els.algo.innerHTML = "";
        files.forEach(function (f) {
          var opt = document.createElement("option");
          opt.value = f.name;
          opt.textContent = f.name;
          opt.title = f.name + " · " + (f.size / 1024).toFixed(1) + " KB";
          els.algo.appendChild(opt);
        });
        if (!files.length) {
          var opt = document.createElement("option");
          opt.value = "";
          opt.textContent = "（当前目录没有可演示的 .py）";
          els.algo.appendChild(opt);
        }
        if (keep) {
          for (var i = 0; i < els.algo.options.length; i++) {
            if (els.algo.options[i].value === keep) { els.algo.selectedIndex = i; break; }
          }
        }
        els.run.disabled = !files.length;
        if (files.length) {
          els.statFile.textContent = els.algo.value || files[0].name;
          els.runinfo.textContent = "共发现 " + files.length + " 个算法文件";
        }
        return files;
      })
      .catch(function (err) {
        logLine("[错误] 无法读取算法列表：" + err, "err");
        setStatus("error", "连接失败");
      });
  }

  /* ---------------- 柱状图渲染（支持一行多个数组 = 多个分组） ---------------- */

  function groupNode(index) {
    var node = els.groups.children[index];
    if (node) return node;
    node = document.createElement("div");
    node.className = "group";
    var label = document.createElement("div");
    label.className = "group-label";
    var bars = document.createElement("div");
    bars.className = "bars";
    node.appendChild(label);
    node.appendChild(bars);
    els.groups.appendChild(node);
    return node;
  }

  function buildBars(barsEl, n) {
    if (barsEl._n === n && barsEl.querySelectorAll(".col").length === (n || 1)) return;
    barsEl.innerHTML = "";
    barsEl._ov = null;          // 重建时叠层也要重新创建
    barsEl._n = n;
    if (!n) {
      var ph = document.createElement("div");
      ph.className = "group-empty";
      ph.textContent = "空";
      barsEl.appendChild(ph);
      return;
    }
    var frag = document.createDocumentFragment();
    for (var i = 0; i < n; i++) {
      var col = document.createElement("div");
      col.className = "col";
      var stack = document.createElement("div");
      stack.className = "stack";
      var val = document.createElement("div");
      val.className = "col-val";
      var bar = document.createElement("div");
      bar.className = "bar";
      var idx = document.createElement("div");
      idx.className = "col-idx";
      idx.textContent = i;
      stack.appendChild(val);
      stack.appendChild(bar);
      col.appendChild(stack);
      col.appendChild(idx);
      frag.appendChild(col);
    }
    barsEl.appendChild(frag);
    barsEl.classList.toggle("dense", n > 24);
  }

  var reduceMotion = false;
  try {
    reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch (e) {}

  /** 交换动画：两根柱子带着自己的数值，横向滑到对方的位置（中间稍微抬起，互不遮挡） */
  function animateSwap(colA, colB, duration) {
    var stackA = colA.children[0];
    var stackB = colB.children[0];
    if (!stackA || !stackB || !stackA.animate) return;
    var dx = colB.offsetLeft - colA.offsetLeft;
    if (!dx) return;

    colA.classList.add("swapping", "over");
    colB.classList.add("swapping");
    var done = function () {
      colA.classList.remove("swapping", "over");
      colB.classList.remove("swapping");
    };

    var base = { duration: duration, easing: "cubic-bezier(.45,.05,.25,1)", fill: "none" };
    var moveA = stackA.animate([
      { transform: "translateX(" + dx + "px) translateY(0px)", offset: 0 },
      { transform: "translateX(" + dx * 0.5 + "px) translateY(-16px)", offset: 0.55 },
      { transform: "translateX(0px) translateY(0px)", offset: 1 }
    ], base);
    var moveB = stackB.animate([
      { transform: "translateX(" + -dx + "px) translateY(0px)", offset: 0 },
      { transform: "translateX(" + -dx * 0.5 + "px) translateY(-6px)", offset: 0.55 },
      { transform: "translateX(0px) translateY(0px)", offset: 1 }
    ], base);
    moveA.onfinish = done;
    moveB.onfinish = done;
  }

  /** 检测“把第 from 个元素抽出来、插到第 to 个位置”的移动（插入排序就是这种） */
  function detectMove(a, b) {
    if (!a || a.length !== b.length || a.length < 2) return null;
    if (!sameSet(a, b)) return null;
    var n = a.length, p = 0, s = 0;
    while (p < n && a[p] === b[p]) p++;
    while (s < n - p && a[n - 1 - s] === b[n - 1 - s]) s++;
    var lo = p, hi = n - 1 - s;
    if (hi <= lo) return null;

    var best = null;
    for (var from = lo; from <= hi; from++) {
      for (var to = lo; to <= hi; to++) {
        if (from === to || a[from] !== b[to]) continue;
        var tmp = a.slice();
        var v = tmp.splice(from, 1)[0];
        tmp.splice(to, 0, v);
        var ok = true;
        for (var k = 0; k < n; k++) { if (tmp[k] !== b[k]) { ok = false; break; } }
        if (!ok) continue;
        // 优先“向右让位、被取元素往左落”的插入形态，其次距离更近的
        var score = (to < from ? 0 : 1000) + Math.abs(from - to);
        if (!best || score < best.score) best = { from: from, to: to, score: score };
      }
    }
    return best;
  }

  /** 取出 → 让位 → 落位：被取的元素先抬起来，中间的格子依次让位，再落到目标位置 */
  function animateMove(cols, from, to, duration, startLift) {
    if (cols.length < 2) return;
    var slotW = cols[1].offsetLeft - cols[0].offsetLeft;
    if (!slotW) return;

    var moved = cols[to];
    var stack = moved.children[0];
    if (!stack || !stack.animate) return;

    var dx = (from - to) * slotW;
    var lift = 30;
    var y0 = startLift ? -lift : 0;
    moved.classList.add("moving");
    var anim = stack.animate([
      { transform: "translate(" + dx + "px, " + y0 + "px)", offset: 0 },
      { transform: "translate(" + dx + "px, " + (-lift - 6) + "px)", offset: startLift ? 0.14 : 0.22 },
      { transform: "translate(" + (dx * 0.15) + "px, " + (-lift - 4) + "px)", offset: 0.72 },
      { transform: "translate(0px, 0px)", offset: 1 }
    ], { duration: duration, easing: "cubic-bezier(.4,0,.25,1)", fill: "none" });
    anim.onfinish = function () { moved.classList.remove("moving"); };

    for (var k = 0; k < cols.length; k++) {
      if (k === to) continue;
      var off = 0;
      if (from > to && k > to && k <= from) off = -slotW;
      else if (from < to && k >= from && k < to) off = slotW;
      if (!off) continue;
      var st = cols[k].children[0];
      if (!st || !st.animate) continue;
      st.animate([
        { transform: "translateX(" + off + "px)" },
        { transform: "translateX(0px)" }
      ], {
        duration: Math.max(140, Math.round(duration * 0.55)),
        delay: Math.round(duration * 0.2),
        easing: "cubic-bezier(.3,.85,.35,1)",
        fill: "backwards"
      });
    }
  }

  /** 打印了“取出元素”但数组还没变的那一帧：把该元素抬出数组，停在空中 */
  function liftColumn(col, lift) {
    var stack = col.children[0];
    if (!stack || !stack.animate) return null;
    col.classList.add("held");
    return stack.animate([
      { transform: "translate(0px, 0px)" },
      { transform: "translate(0px, " + -lift + "px)" }
    ], { duration: 280, fill: "forwards", easing: "cubic-bezier(.34,1.3,.5,1)" });
  }

  var HELD_LABEL_RE = /取出|拿出|抽出|拿出|当前|待插入|插入值|临时|key|value|temp|hold|picked/i

  /* ---------------- 结构标注：间隔分组 / 桶边界 / 堆与已排序区 ---------------- */

  var SVG_NS = "http://www.w3.org/2000/svg";

  function decorate(barsEl, cols, values, struct) {
    if (!barsEl._ov) {
      var layer = document.createElement("div");
      layer.className = "overlay";
      barsEl.appendChild(layer);
      barsEl._ov = layer;
    }
    var ov = barsEl._ov;
    ov.innerHTML = "";

    var n = cols.length;
    var i;
    var gap = struct && struct.gap;
    var gapped = !!(gap && gap > 1 && gap < n);
    barsEl.classList.toggle("gapped", gapped);

    for (i = 0; i < n; i++) {
      cols[i].classList.remove("tail", "cur");
      if (gapped) {
        var hue = Math.round((i % gap) * 360 / gap);
        cols[i].style.setProperty("--g1", "hsl(" + hue + " 90% 66%)");
        cols[i].style.setProperty("--g2", "hsl(" + hue + " 78% 46%)");
      } else {
        cols[i].style.removeProperty("--g1");
        cols[i].style.removeProperty("--g2");
      }
    }

    // 桶：每个桶一段浅色带 + 虚线边界 + 桶号
    if (struct && struct.buckets > 1 && n) {
      var k = struct.buckets;
      var bucketOf = function (v) {
        return Math.max(0, Math.min(k - 1, Math.floor((v - 1) * k / 100)));
      };
      var startIdx = 0;
      var curB = bucketOf(values[0]);
      for (i = 1; i <= n; i++) {
        var b = i < n ? bucketOf(values[i]) : -1;
        if (b === curB) continue;
        var l = cols[startIdx].offsetLeft;
        var r = cols[i - 1].offsetLeft + cols[i - 1].offsetWidth;
        var band = document.createElement("i");
        band.className = "bucket-band" + (curB % 2 ? " odd" : "");
        band.style.left = (l - 2) + "px";
        band.style.width = (r - l + 4) + "px";
        ov.appendChild(band);
        var tag = document.createElement("span");
        tag.className = "bucket-tag";
        tag.textContent = "桶" + curB;
        tag.style.left = (l + 2) + "px";
        ov.appendChild(tag);
        if (i < n) {
          var div = document.createElement("i");
          div.className = "bucket-div";
          div.style.left = (cols[i].offsetLeft - 2) + "px";
          ov.appendChild(div);
        }
        startIdx = i;
        curB = b;
      }
    }

    // 堆：树连线 + 堆区/已排序区分界 + 当前结点
    if (struct && typeof struct.heap === "number" && struct.heap >= 0 && struct.heap <= n) {
      var hn = struct.heap;
      for (i = hn; i < n; i++) cols[i].classList.add("tail");
      if (hn > 0 && hn < n) {
        var hd = document.createElement("i");
        hd.className = "heap-div";
        hd.style.left = (cols[hn].offsetLeft - 2) + "px";
        ov.appendChild(hd);
        var ht = document.createElement("span");
        ht.className = "heap-tag";
        ht.textContent = "右边已排好序";
        ht.style.left = (cols[hn].offsetLeft + 3) + "px";
        ov.appendChild(ht);
      }
      if (hn >= 2) {
        var svg = document.createElementNS(SVG_NS, "svg");
        svg.setAttribute("class", "tree-lines");
        for (var p = 0; p < hn; p++) {
          for (var q = 0; q < 2; q++) {
            var child = 2 * p + 1 + q;
            if (child >= hn) continue;
            var px = cols[p].offsetLeft + cols[p].offsetWidth / 2;
            var cx = cols[child].offsetLeft + cols[child].offsetWidth / 2;
            var path = document.createElementNS(SVG_NS, "path");
            path.setAttribute("d", "M" + cx + " 27 C" + cx + " 14, " + px + " 16, " + px + " 3");
            svg.appendChild(path);
          }
        }
        ov.appendChild(svg);
      }
    }

    if (struct && typeof struct.cur === "number" && cols[struct.cur]) {
      cols[struct.cur].classList.add("cur");
    }
  }

  function renderGroup(node, values, prevValues, min, max, animate, heldValue, struct) {
    var barsEl = node.children[1];

    buildBars(barsEl, values.length);

    if (!values.length) return;
    barsEl.classList.remove("sorted");

    var base = Math.min(0, min);
    var span = max - base;
    var extraTop = 0;
    if (struct) {
      if (typeof struct.heap === "number") extraTop = 30;   // 顶部留给树的连线
      else if (struct.buckets > 1) extraTop = 14;
    }
    var labelSpace = (barsEl.classList.contains("dense") ? 20 : 46) + extraTop;
    var avail = Math.max(24, barsEl.clientHeight - labelSpace);

    var cols = barsEl.querySelectorAll(".col");
    var pace = parseInt(els.pace.value, 10) || 700;
    var swapDur = Math.max(180, Math.min(620, Math.round(pace * 0.8)));

    // 找出这一帧变化的列；刚好两根且互换数值 = 一次 swap
    var changedIdx = [];
    if (prevValues) {
      for (var c = 0; c < values.length; c++) {
        if (prevValues[c] !== values[c]) changedIdx.push(c);
      }
    }
    var isSwap = changedIdx.length === 2 &&
      values[changedIdx[0]] === prevValues[changedIdx[1]] &&
      values[changedIdx[1]] === prevValues[changedIdx[0]];

    var swapSet = isSwap ? [changedIdx[0], changedIdx[1]] : [];
    var move = (!isSwap && changedIdx.length >= 2) ? detectMove(prevValues, values) : null;
    var moveSet = [];
    if (move) {
      var lo = Math.min(move.from, move.to), hi = Math.max(move.from, move.to);
      for (var m = lo; m <= hi; m++) moveSet.push(m);
    }

    // 这一帧是否带“取出元素”，以及它在数组里的位置
    var heldIndex = -1;
    if (heldValue !== null && heldValue !== undefined) {
      for (var hh = values.length - 1; hh >= 0; hh--) {
        if (values[hh] === heldValue) { heldIndex = hh; break; }
      }
    }

    // 上一帧抬起的手持元素：这一帧要么继续抬着，要么交给移动动画接力
    var startLift = false;
    if (st.held) {
      if (move && st.held.index === move.from) startLift = true;
      if (st.held.anim) { try { st.held.anim.cancel(); } catch (e) {} }
      var oldCol = cols[st.held.index];
      if (oldCol) oldCol.classList.remove("held");
      st.held = null;
    }

    for (var i = 0; i < values.length; i++) {
      var col = cols[i];
      var stack = col.children[0];
      var bar = stack.children[1];
      var ratio = span > 0 ? (values[i] - base) / span : 0.6;
      var p = Math.max(0.02, Math.min(1, ratio));

      stack.children[0].textContent = fmt(values[i]);

      if (isSwap || (move && moveSet.indexOf(i) >= 0)) {
        // 交换/移动帧：柱子的高度要立刻到位，不能一边滑一边变形
        bar.style.transition = "none";
        bar.style.height = Math.round(p * avail) + "px";
        void bar.offsetWidth;
        bar.style.transition = "";
      } else {
        bar.style.height = Math.round(p * avail) + "px";
      }

      var changed = prevValues && i < prevValues.length && prevValues[i] !== values[i];
      if (changed) {
        col.classList.add("changed");
        (function (n, ms) {
          clearTimeout(n._hl);
          n._hl = setTimeout(function () { n.classList.remove("changed"); }, ms);
        })(col, (isSwap || move) ? swapDur + 320 : 620);
      } else {
        clearTimeout(col._hl);
        col.classList.remove("changed");
      }
    }

    if (animate !== false && !reduceMotion) {
      if (isSwap) animateSwap(cols[changedIdx[0]], cols[changedIdx[1]], swapDur);
      else if (move) animateMove(cols, move.from, move.to, Math.round(swapDur * 1.05), startLift);
      else if (heldIndex >= 0) {
        st.held = { index: heldIndex, value: values[heldIndex], anim: liftColumn(cols[heldIndex], 34) };
      }
    }

    decorate(barsEl, cols, values, struct);
  }

  /* ---------------- 递归分区树 ----------------
     一行多个数组 = 一次分区（左 / 基准 / 右）。按前序顺序把每帧挂到
     对应父区段下面，用虚线分隔各列表，所有递归层同时可见。          */

  function sameSet(a, b) {
    if (a.length !== b.length) return false;
    var x = a.slice().sort(function (p, q) { return p - q; });
    var y = b.slice().sort(function (p, q) { return p - q; });
    for (var i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
    return true;
  }

  function makeNode(left, mid, right, pivot, depth, x0, x1, parent) {
    var n = left.length + mid.length + right.length;
    var w = x1 - x0;
    var u = n ? w / n : 0;
    return {
      left: left, mid: mid, right: right, pivot: pivot,
      depth: depth, x0: x0, x1: x1, parent: parent,
      leftSpan: [x0, x0 + left.length * u],
      rightSpan: [x1 - right.length * u, x1],
      leftFilled: false, rightFilled: false,
      el: null
    };
  }

  function splitFrame(frame) {
    var arrays = frame.arrays || [];
    var left = arrays.length ? (arrays[0].values || []) : [];
    var right = arrays.length ? (arrays[arrays.length - 1].values || []) : [];
    var mid = [];
    for (var i = 1; i < arrays.length - 1; i++) mid = mid.concat(arrays[i].values || []);
    var pivot = (frame.scalars && frame.scalars.length) ? frame.scalars[0].value
              : (mid.length === 1 ? mid[0] : null);
    // 只有 left/right 时，基准本身不在任何列表里，补成一格
    if (!mid.length && pivot !== null) mid = [pivot];
    return { left: left, mid: mid, right: right, pivot: pivot,
             all: left.concat(mid, right) };
  }

  function treeRootSize() {
    var r = st.tree.root;
    if (!r) return 0;
    return r.left.length + r.mid.length + r.right.length;
  }

  function renderScalars(frame) {
    els.scalars.innerHTML = "";
    (frame.scalars || []).forEach(function (s) {
      var chip = document.createElement("span");
      chip.className = "chip scalar";
      chip.appendChild(document.createTextNode((s.label || "?") + " = "));
      var b = document.createElement("b");
      b.textContent = fmt(s.value);
      chip.appendChild(b);
      els.scalars.appendChild(chip);
    });
  }

  function treeBarPx(v) {
    var t = st.tree;
    var base = Math.min(0, t.min);
    var span = t.max - base;
    var r = span > 0 ? (v - base) / span : 0.6;
    return Math.max(3, Math.round(Math.max(0.02, Math.min(1, r)) * TREE_BAR_MAX));
  }

  function renderNodeEl(node, opts) {
    opts = opts || {};
    var el = document.createElement("div");
    el.className = "node" + (opts.cls ? " " + opts.cls : "");
    el.style.left = (node.x0 * 100).toFixed(3) + "%";
    el.style.width = Math.max(0.4, (node.x1 - node.x0) * 100).toFixed(3) + "%";
    el.style.top = (node.depth * TREE_ROW_H) + "px";

    var specs = [
      { cls: "left", name: "\u5de6", values: node.left },
      { cls: "mid", name: "\u57fa\u51c6", values: node.mid },
      { cls: "right", name: "\u53f3", values: node.right }
    ];
    specs.forEach(function (sp) {
      var seg = document.createElement("div");
      seg.className = "seg seg-" + sp.cls;
      seg.style.flexGrow = String(sp.values.length);
      if (!sp.values.length) {
        seg.style.display = "none";
        return;
      }
      var barsEl = document.createElement("div");
      barsEl.className = "seg-bars";
      sp.values.forEach(function (v) {
        var tb = document.createElement("div");
        tb.className = "tbar";
        var val = document.createElement("span");
        val.className = "tval";
        val.textContent = fmt(v);
        var fill = document.createElement("i");
        fill.className = "tfill";
        fill.style.height = treeBarPx(v) + "px";
        tb.appendChild(val);
        tb.appendChild(fill);
        barsEl.appendChild(tb);
      });
      var name = document.createElement("div");
      name.className = "seg-name";
      name.textContent = sp.name + "\u00b7" + sp.values.length;
      seg.appendChild(barsEl);
      seg.appendChild(name);
      el.appendChild(seg);
    });

    (opts.parentEl || els.treeInner).appendChild(el);

    // 窄区段里数字放不下就隐藏数值标签
    requestAnimationFrame(function () {
      var segs = el.querySelectorAll(".seg");
      for (var i = 0; i < segs.length; i++) {
        var seg = segs[i];
        var count = seg.querySelectorAll(".tbar").length;
        if (count && seg.clientWidth / count < 26) seg.classList.add("dense");
      }
    });
    return el;
  }

  function treeScrollToNewest() {
    var t = st.tree;
    if (!t.nodes.length) return;
    var newest = t.nodes[t.nodes.length - 1];
    var top = newest.depth * TREE_ROW_H;
    var view = els.tree.clientHeight;
    if (top + TREE_ROW_H > els.tree.scrollTop + view) {
      els.tree.scrollTop = Math.max(0, top + TREE_ROW_H - view + 12);
    }
  }

  function treeMarkPath(node) {
    var t = st.tree;
    var path = [];
    var cur = node;
    while (cur) { path.push(cur); cur = cur.parent; }
    t.nodes.forEach(function (nd) {
      nd.el.classList.remove("fresh", "active", "idle");
      if (nd === node) nd.el.classList.add("fresh", "active");
      else if (path.indexOf(nd) >= 0) nd.el.classList.add("active");
      else nd.el.classList.add("idle");
    });
  }

  function treeGrow(frame, node) {
    var t = st.tree;
    t.nodes.push(node);
    node.el = renderNodeEl(node);
    t.depth = Math.max(t.depth, node.depth);
    els.treeInner.style.height = ((t.depth + 1) * TREE_ROW_H + 8) + "px";
    treeMarkPath(node);
    treeScrollToNewest();
  }

  function treeRestart(frame) {
    var t = st.tree;
    els.treeInner.innerHTML = "";
    t.nodes = [];
    t.stack = [];
    t.depth = 0;
    t.result = null;

    var split = splitFrame(frame);
    t.min = split.all.length ? Math.min.apply(null, split.all) : 0;
    t.max = split.all.length ? Math.max.apply(null, split.all) : 1;
    var root = makeNode(split.left, split.mid, split.right, split.pivot, 0, 0, 1, null);
    t.root = root;
    t.stack = [root];
    treeGrow(frame, root);
  }

  /** 返回 true 表示这帧被画进了递归树 */
  function treePush(frame) {
    var t = st.tree;
    if (!t.active) {
      t.active = true;
      els.groups.classList.add("off");
      els.tree.classList.add("on");
      treeRestart(frame);
      return true;
    }
    var split = splitFrame(frame);

    while (t.stack.length) {
      var top = t.stack[t.stack.length - 1];
      if (!top.leftFilled) {
        if (top.left.length <= 1) { top.leftFilled = true; continue; }
        if (sameSet(top.left, split.all)) {
          var ln = makeNode(split.left, split.mid, split.right, split.pivot,
                            top.depth + 1, top.leftSpan[0], top.leftSpan[1], top);
          top.leftFilled = true;
          t.stack.push(ln);
          treeGrow(frame, ln);
          return true;
        }
        treeRestart(frame);
        return true;
      }
      if (!top.rightFilled) {
        if (top.right.length <= 1) { top.rightFilled = true; continue; }
        if (sameSet(top.right, split.all)) {
          var rn = makeNode(split.left, split.mid, split.right, split.pivot,
                            top.depth + 1, top.rightSpan[0], top.rightSpan[1], top);
          top.rightFilled = true;
          t.stack.push(rn);
          treeGrow(frame, rn);
          return true;
        }
        treeRestart(frame);
        return true;
      }
      t.stack.pop();
    }
    treeRestart(frame);
    return true;
  }

  function treeSetResult(values) {
    var t = st.tree;
    if (t.resultEl) { t.resultEl.parentNode.removeChild(t.resultEl); t.resultEl = null; }
    if (!values || !values.length) return;
    var node = { left: values, mid: [], right: [], depth: t.depth + 1, x0: 0, x1: 1 };
    t.result = values;
    t.resultEl = renderNodeEl(node, { cls: "result" });
    t.resultEl.style.top = ((t.depth + 1) * TREE_ROW_H) + "px";
    t.resultEl.style.width = "100%";
    t.resultEl.style.left = "0%";
    t.resultEl.querySelectorAll(".seg-name").forEach(function (n) {
      n.textContent = "\u6700\u7ec8\u7ed3\u679c";
    });
    els.treeInner.style.height = ((t.depth + 2) * TREE_ROW_H + 12) + "px";
    treeScrollToNewest();
  }

  function treeReset() {
    st.tree = { active: false, root: null, stack: [], nodes: [], min: 0, max: 1, depth: 0, result: null, resultEl: null };
    els.treeInner.innerHTML = "";
    els.tree.scrollTop = 0;
    els.tree.classList.remove("on");
    els.groups.classList.remove("off");
  }

  function renderFrame(frame, redraw, animate) {
    var arrays = (frame.arrays && frame.arrays.length)
      ? frame.arrays
      : (Array.isArray(frame.values) && frame.values.length
          ? [{ label: "", values: frame.values }]
          : []);

    // 只打印了“取出元素: 65”这种没有数组的行：保持画面，把那根柱子抬起来
    if (!arrays.length) {
      var held = (frame.scalars || [])[0];
      var lastOne = (st.lastArrays && st.lastArrays.length === 1) ? st.lastArrays[0] : null;
      if (held && lastOne && els.groups.children[0]) {
        var wrap = els.groups.children[0].children[1];
        var vals0 = lastOne.values || [];
        var idx0 = -1;
        for (var q = vals0.length - 1; q >= 0; q--) {
          if (vals0[q] === held.value) { idx0 = q; break; }
        }
        // 这个数不在当前数组里 = 只是一行普通数字输出，不当成“取出”
        if (idx0 < 0) { logLine("[跳过] " + fmt(held.value) + " 不在当前数组中，按普通输出忽略", "sys"); return; }

        if (st.held) {
          if (st.held.anim) { try { st.held.anim.cancel(); } catch (e) {} }
          var heldCols = wrap.querySelectorAll(".col");
          if (heldCols[st.held.index]) heldCols[st.held.index].classList.remove("held");
          st.held = null;
        }
        var col0 = wrap.children[idx0];
        if (col0 && !reduceMotion) {
          st.held = { index: idx0, value: held.value, anim: liftColumn(col0, 34) };
        }
        renderScalars(frame);
        els.chipFrame.textContent = "第 " + frame.i + " 帧";
        if (!redraw) { st.viewed++; els.statFrames.textContent = st.viewed; }
        els.caption.textContent = frame.caption || ("第 " + frame.i + " 帧 · 取出 " + fmt(held.value) + "，准备插入");
      }
      return;
    }

    els.empty.classList.add("hide");

    // 一行多个数组 = 一次分区，走递归树视图（所有层级的列表同时展示）
    if (arrays.length >= 2) {
      treePush(frame);
      st.changes++;
      if (!redraw) {
        st.viewed++;
        st.last = frame;
        st.lastArrays = arrays;
      }
      els.chipN.textContent = treeRootSize() + " 个元素 · 递归分区";
      els.chipFrame.textContent = "第 " + frame.i + " 帧";
      els.statN.textContent = treeRootSize() + " 个 · 深度 " + st.tree.depth;
      els.statFrames.textContent = st.viewed;
      els.statSwaps.textContent = st.changes;
      els.statRange.textContent = fmt(st.tree.min) + " ~ " + fmt(st.tree.max);
      renderScalars(frame);
      if (frame.caption) {
        st.keepLast = frame.caption;
        els.caption.textContent = frame.caption;
      } else if (!redraw) {
        els.caption.textContent = "第 " + frame.i + " 帧 · 递归深度 " + st.tree.depth + " · 正在分区";
      }
      return;
    }

    // 树已经建好后出现的单数组 = 最终结果，贴在树的最下面
    if (st.tree.active) {
      treeSetResult(arrays[0].values || []);
      if (!redraw) {
        st.viewed++;
        st.last = frame;
        st.lastArrays = arrays;
      }
      els.chipFrame.textContent = "第 " + frame.i + " 帧";
      els.statFrames.textContent = st.viewed;
      renderScalars(frame);
      if (!st.done) els.caption.textContent = "递归结束 · 最终结果";
      return;
    }

    var groups = els.groups.children;
    while (groups.length > arrays.length) els.groups.removeChild(els.groups.lastChild);

    var prevArrays = st.lastArrays || [];
    var all = [];
    var total = 0;
    var changedAny = false;
    var min = Infinity, max = -Infinity;

    for (var g = 0; g < arrays.length; g++) {
      groupNode(g);
      var values = arrays[g].values || [];
      for (var v = 0; v < values.length; v++) {
        all.push(values[v]);
        if (values[v] < min) min = values[v];
        if (values[v] > max) max = values[v];
      }
      total += values.length;
    }
    if (!all.length) { min = 0; max = 0; }

    void els.groups.offsetHeight; // 先算一次布局，保证高度过渡生效

    // 结构标注：间隔 / 堆 / 桶，来自算法打印的标量
    var struct = { gap: null, heap: null, cur: null, buckets: null };
    (frame.scalars || []).forEach(function (sc) {
      var label = String((sc && sc.label) || "");
      if (/间隔|gap/i.test(label)) struct.gap = Math.round(sc.value);
      else if (/堆大小|堆长|heap/i.test(label)) struct.heap = Math.round(sc.value);
      else if (/桶数|buckets?/i.test(label)) struct.buckets = Math.round(sc.value);
      else if (/结点|节点|node/i.test(label)) struct.cur = Math.round(sc.value);
    });

    // 算法若打印了“取出元素: v”，把 v 从数组里抬起来
    var heldValue = null;
    (frame.scalars || []).forEach(function (sc) {
      var label = String((sc && sc.label) || "");
      if (heldValue === null && HELD_LABEL_RE.test(label) && !/间隔|桶数|堆大小|结点|节点/.test(label)) {
        heldValue = sc.value;
      }
    });

    for (var h = 0; h < arrays.length; h++) {
      var nd = els.groups.children[h];
      var vals = arrays[h].values || [];
      var prev = prevArrays[h] && prevArrays[h].values;
      var labelEl = nd.children[0];
      var name = arrays[h].label || "";
      var single = arrays.length === 1;
      // 分组宽度按元素个数的平方根分配，避免“1 个元素”和“8 个元素”一样宽
      nd.style.flexGrow = String(single ? 1 : Math.max(1, Math.sqrt(vals.length || 1)).toFixed(3));
      if (!name && single) {
        labelEl.textContent = "";
      } else {
        labelEl.textContent = "";
        if (name) labelEl.appendChild(document.createTextNode(name));
        var cnt = document.createElement("span");
        cnt.className = "cnt";
        cnt.textContent = (name ? " " : "") + "(" + vals.length + ")";
        labelEl.appendChild(cnt);
      }
      renderGroup(nd, vals, prev && prev.length === vals.length ? prev : null, min, max, animate,
        arrays.length === 1 ? heldValue : null, struct);
      if (prev && prev.length === vals.length && vals.length) {
        for (var c = 0; c < vals.length; c++) if (prev[c] !== vals[c]) changedAny = true;
      }
    }

    if (!redraw) {
      if (changedAny) st.changes++;
      st.last = frame;
      st.lastArrays = arrays;
      st.viewed++;
      els.statFrames.textContent = st.viewed;
      els.statSwaps.textContent = st.changes;
    } else {
      st.lastArrays = arrays;
    }

    els.chipN.textContent = total + " 个元素" + (arrays.length > 1 ? " · " + arrays.length + " 组" : "");
    els.chipFrame.textContent = "第 " + frame.i + " 帧";
    els.statN.textContent = total + " 个" + (arrays.length > 1 ? " · " + arrays.length + " 组" : "");
    els.statRange.textContent = all.length ? (fmt(min) + " ~ " + fmt(max)) : "—";

    // 标量（如 pivot = 5）显示为小徽章
    renderScalars(frame);

    if (frame.caption) {
      st.keepLast = frame.caption;
      els.caption.textContent = frame.caption;
    } else if (!redraw) {
      if (arrays.length > 1) {
        els.caption.textContent = "第 " + frame.i + " 帧 · 分区中…";
      } else if (heldValue !== null && heldValue !== undefined) {
        els.caption.textContent = "第 " + frame.i + " 帧 · 取出 " + fmt(heldValue) + "，准备插入";
      } else {
        els.caption.textContent = "第 " + frame.i + " 帧 · 排序中…";
      }
    }
  }

  /* ---------------- 播放队列 ---------------- */

  function enqueue(frame) {
    st.frameSeq++;
    st.queue.push(frame);
    if (st.queue.length > 800) st.queue.splice(0, st.queue.length - 800);
    if (!st.timer && st.running) pump();
  }

  function pump() {
    st.timer = null;
    if (!st.running) return;

    var base = parseInt(els.pace.value, 10) || 700;
    var backlog = st.queue.length;

    if (backlog > 400) {
      // 帧量爆炸（算法在刷屏，比如递归没收敛）：直接跳到最新一帧，避免卡住界面
      var latest = st.queue[backlog - 1];
      st.queue.length = 0;
      renderFrame(latest, false, false);
      st.timer = setTimeout(pump, base);
      return;
    }

    if (!backlog) {
      if (st.done) { finalize(); return; }
      st.timer = setTimeout(pump, base);
      return;
    }

    // 积压多时只缩短每帧停留时间，绝不跳帧——否则会把“取出/插入”这类关键帧丢掉
    var remain = (backlog - 1) * base;
    var budget = 18000;
    var wait = base;
    if (remain > budget) wait = Math.max(70, Math.floor(base * budget / remain));

    renderFrame(st.queue.shift(), false, wait >= 180);
    st.timer = setTimeout(pump, wait);
  }

  function finalize() {
    if (st.timer) { clearTimeout(st.timer); st.timer = null; }
    if (st.clock) { clearInterval(st.clock); st.clock = null; }
    st.running = false;
    st.queue.length = 0;

    els.run.disabled = false;
    els.stop.disabled = true;
    els.runinfo.textContent = "完成 · 渲染 " + st.viewed + " 帧 · 算法运行 " + (st.algTime || 0).toFixed(2) + " s";
    els.statTime.textContent = (st.algTime || 0).toFixed(2) + " s";

    if (st.tree.active) {
      st.tree.nodes.forEach(function (nd) {
        if (!nd.el) return;
        nd.el.classList.remove("fresh", "idle", "active");
      });
    }

    var lastArrays = st.lastArrays || [];
    var values = (lastArrays.length === 1 ? lastArrays[0].values : []) || [];
    var treeResult = st.tree.active ? (st.tree.result || []) : [];
    if (!values.length && treeResult.length) values = treeResult;

    var markSorted = function () {
      if (st.tree.active && st.tree.resultEl) {
        st.tree.resultEl.classList.add("active");
        return;
      }
      for (var i = 0; i < els.groups.children.length; i++) {
        var barsEl = els.groups.children[i].children[1];
        barsEl.classList.add("sorted");
        var cols = barsEl.querySelectorAll(".col");
        for (var c = 0; c < cols.length; c++) {
          cols[c].classList.remove("changed");
          clearTimeout(cols[c]._hl);
        }
      }
    };
    if (values.length && isSortedAsc(values)) {
      markSorted();
      setStatus("done", "完成");
      els.caption.textContent = "✓ 升序排序完成 · 共 " + values.length + " 个元素";
    } else if (values.length && isSortedDesc(values)) {
      markSorted();
      setStatus("done", "完成");
      els.caption.textContent = "✓ 降序排序完成 · 共 " + values.length + " 个元素";
    } else if (st.exitCode === 0 && !st.viewed) {
      setStatus("idle", "无输出");
      els.caption.textContent = "脚本运行结束，但没有解析到数组输出（试试 print(列表)）";
    } else if (st.exitCode === 0) {
      setStatus("idle", "结束");
      els.caption.textContent = st.keepLast || "演示结束";
    } else {
      setStatus("error", "异常退出");
      els.caption.textContent = "算法进程异常退出（退出码 " + st.exitCode + "），详见右侧输出";
    }

    if (st.exitCode === 0) {
      logLine("[完成] 进程正常退出 · 共 " + st.frameSeq + " 帧 · 算法实际运行 " + (st.algTime || 0).toFixed(2) + " s", "sys");
    } else {
      logLine("[失败] 进程退出码 " + st.exitCode, "err");
    }
  }

  /* ---------------- 运行 / 停止 ---------------- */

  function resetStage() {
    if (st.timer) { clearTimeout(st.timer); st.timer = null; }
    if (st.held) {
      if (st.held.anim) { try { st.held.anim.cancel(); } catch (e) {} }
      st.held = null;
    }
    st.queue.length = 0;
    st.last = null;
    st.lastArrays = null;
    st.viewed = 0;
    st.changes = 0;
    st.frameSeq = 0;
    st.done = false;
    st.exitCode = null;
    st.algTime = 0;
    st.keepLast = false;
    els.groups.innerHTML = "";
    els.scalars.innerHTML = "";
    treeReset();
    els.empty.classList.remove("hide");
    els.chipN.textContent = "— 个元素";
    els.chipFrame.textContent = "第 0 帧";
    els.statN.textContent = "—";
    els.statFrames.textContent = "0";
    els.statSwaps.textContent = "0";
    els.statRange.textContent = "—";
    els.statTime.textContent = "—";
    els.caption.textContent = "正在启动算法进程…";
  }

  function startRun() {
    var file = els.algo.value;
    if (!file) {
      setStatus("error", "无文件");
      els.caption.textContent = "当前目录没有可演示的 .py 文件";
      return;
    }
    stopRun(false, true);
    resetStage();

    st.running = true;
    st.token = (self.crypto && crypto.randomUUID) ? crypto.randomUUID()
             : "t" + Date.now() + Math.random().toString(16).slice(2);

    els.run.disabled = true;
    els.stop.disabled = false;
    els.statFile.textContent = file;
    setStatus("running", "运行中");
    els.caption.textContent = "正在运行 " + file + " …";
    els.log.textContent = "";

    st.clock = setInterval(tickClock, 180);

    var url = "/api/run?file=" + encodeURIComponent(file) +
              "&speed=" + encodeURIComponent(els.speed.value) +
              "&token=" + encodeURIComponent(st.token);

    var es = new EventSource(url);
    st.es = es;

    es.addEventListener("meta", function (e) {
      var d = JSON.parse(e.data);
      logLine("[运行] " + d.file + " · 速度 " + d.speed + "×", "sys");
      logLine("[命令] " + d.command, "sys");
    });

    es.addEventListener("log", function (e) {
      var d = JSON.parse(e.data);
      logLine(d.text, d.stream === "stderr" ? "err" : "");
    });

    es.addEventListener("frame", function (e) {
      var d = JSON.parse(e.data);
      // 服务端上报的是“算法进程已运行多少秒”，计时统一用这个
      if (typeof d.t === "number" && d.t > st.algTime) st.algTime = d.t;
      enqueue(d);
      if (d.caption) st.keepLast = d.caption;
    });

    es.addEventListener("failed", function (e) {
      var d = JSON.parse(e.data);
      logLine("[错误] " + d.message, "err");
      st.done = true;
      st.exitCode = -1;
    });

    es.addEventListener("done", function (e) {
      var d = JSON.parse(e.data);
      st.done = true;
      st.exitCode = d.code;
      st.algTime = d.elapsed;
      els.statTime.textContent = d.elapsed.toFixed(2) + " s";
      closeStream();
      els.run.disabled = false;
      if (st.queue.length === 0 && !st.timer) pump();
    });

    es.onerror = function () {
      if (st.done) { closeStream(); return; }
      closeStream();
      if (st.running) {
        st.done = true;
        st.exitCode = -1;
        logLine("[错误] 与演示服务的连接中断", "err");
        if (st.queue.length === 0 && !st.timer) pump();
      }
    };
  }

  function closeStream() {
    if (st.es) { try { st.es.close(); } catch (e) {} st.es = null; }
  }

  function stopRun(notify, silent) {
    if (st.es) closeStream();
    if (st.token) {
      fetch("/api/stop?token=" + encodeURIComponent(st.token), { cache: "no-store" }).catch(function () {});
      st.token = null;
    }
    var wasRunning = st.running;
    st.running = false;
    st.done = true;
    st.queue.length = 0;
    if (st.timer) { clearTimeout(st.timer); st.timer = null; }
    if (st.clock) { clearInterval(st.clock); st.clock = null; }
    els.run.disabled = false;
    els.stop.disabled = true;
    if (wasRunning && notify) {
      setStatus("idle", "已停止");
      els.caption.textContent = "已停止演示";
      els.runinfo.textContent = "已停止";
      logLine("[停止] 演示已手动停止", "sys");
    }
  }

  /* ---------------- 事件绑定 ---------------- */

  els.run.addEventListener("click", startRun);
  els.stop.addEventListener("click", function () { stopRun(true, false); });
  els.refresh.addEventListener("click", function () {
    loadAlgorithms().then(function () { els.runinfo.textContent = "已重新扫描目录"; });
  });
  els.clear.addEventListener("click", function () { els.log.textContent = ""; });
  els.algo.addEventListener("change", function () {
    els.statFile.textContent = els.algo.value;
  });

  // 柱高是按像素算的，窗口尺寸变化时按当前帧重画一次（不计入统计）
  var resizeTimer = null;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      if (st.last) renderFrame(st.last, true);
    }, 140);
  });

  window.addEventListener("beforeunload", function () {
    if (st.token) {
      try {
        navigator.sendBeacon("/api/stop?token=" + encodeURIComponent(st.token));
      } catch (e) {}
    }
  });

  loadAlgorithms();
})();
</script>
</body>
</html>
"""


# --------------------------------------------------------------------------
# 启动
# --------------------------------------------------------------------------


def say(message=""):
    try:
        print(message, flush=True)
    except Exception:
        pass


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) >= 2 and argv[0] == "--run":
        speed = 1.0
        if len(argv) > 2:
            try:
                speed = float(argv[2])
            except ValueError:
                speed = 1.0
        run_script(argv[1], speed)
        return 0

    parser = argparse.ArgumentParser(description="算法可视化演示器（直接运行你的 .py 算法文件）")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="监听端口，默认 8000（被占用时自动顺延）")
    parser.add_argument("--no-browser", action="store_true", help="启动后不自动打开浏览器")
    args = parser.parse_args(argv)

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    httpd = None
    port = args.port
    last_error = None
    for candidate in range(args.port, args.port + 20):
        try:
            httpd = VizServer((args.host, candidate), Handler)
            port = candidate
            break
        except OSError as exc:
            last_error = exc
            continue

    if httpd is None:
        speak = "无法绑定端口 %d ~ %d：%s" % (args.port, args.port + 19, last_error)
        say(speak)
        return 1

    httpd.daemon_threads = True
    url = "http://%s:%d/" % ("127.0.0.1" if args.host in ("0.0.0.0", "") else args.host, port)

    files = list_algorithms()
    say("")
    say("  算法可视化演示器  ·  纯演示层，不修改任何算法源码")
    say("  " + "-" * 52)
    say("  目录    : %s" % BASE_DIR)
    say("  页面    : %s" % url)
    say("  已发现  : %d 个算法文件" % len(files))
    for item in files:
        say("            - %s" % item["name"])
    if not files:
        say("            （把要演示的 .py 放在本文件同级目录即可）")
    say("  " + "-" * 52)
    say("  按 Ctrl+C 停止服务")
    say("")

    if not args.no_browser:
        threading.Timer(0.7, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever(poll_interval=0.3)
    except KeyboardInterrupt:
        say("\n正在停止服务…")
    finally:
        with _RUNS_LOCK:
            procs = list(_RUNS.values())
        for proc in procs:
            _terminate(proc)
        httpd.server_close()
        say("已停止。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
