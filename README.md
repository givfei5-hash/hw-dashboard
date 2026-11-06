# 硬件 + 推理引擎看板（hw-dashboard）

单文件 Python 标准库后端 + 自包含前端（无 CDN、无第三方包），16:9 一屏看板：
KPI 带、CPU 逐线程块（行列随线程数与卡宽自适应）、内存/引擎图、8 卡 GPU 行
（0.7s 快采的「正在出力」染色）、引擎实时与配置、最近请求、三套主题（石墨/纸白/终端）。

## 运行前提

- Linux，NVIDIA GPU（`nvidia-smi` 可用）
- 可选：推理引擎（Strata 或 Maya 兼容接口）在 `127.0.0.1:8080` 暴露 `/metrics`，
  或经任意代理转发到 `127.0.0.1:8000`。没有引擎也能跑，引擎相关卡片会显示不可用，
  硬件部分完全正常。

## 跑起来

```bash
# 把 index.html 与 hw_dashboard.py 放同一目录，然后：
python3 hw_dashboard.py          # 默认 0.0.0.0:8092
```

浏览器开 `http://<机器IP>:8092/`。

环境变量（都可以不设）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `HW_DASH_PORT` | 8092 | 监听端口 |
| `HW_DASH_ENGINE` | http://127.0.0.1:8000 | 引擎 metrics 来源（先试这个，失败再直连 :8080） |
| `HW_DASH_GPU_FAST` | 0.7 | GPU 快采周期（秒），设 0 关掉 |

systemd：参考 `hw-dashboard.service` 模板，改好 User 与路径后
`sudo cp hw-dashboard.service /etc/systemd/system/ && sudo systemctl enable --now hw-dashboard`。

## 页面操作

- `[1]~[5]` 时间窗（15m/1h/6h/24h/7d），`[6]~[8]` 铺满/滚动/自动，`[9]~[11]` 三套主题
- `?probe=1` 自检：把各卡片的实测位置与数据点数写进 `data-probe` 属性，排查排版用

## 数据与隐私

- 采集只读（/proc、/sys、nvidia-smi、引擎 metrics），不启停任何服务
- SQLite 本地库 `hw_history.db`，快照/请求/事件各保留 14 天，到点自动清理
- 本发行版已去标识：不含主机名、内网地址、用户路径；页面里所有链接按你访问用的
  地址动态生成，后端路径一律从用户主目录展开
