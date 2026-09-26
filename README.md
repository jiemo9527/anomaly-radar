# 异动雷达 anomaly-radar

对 Binance USDT 本位永续合约做全市场实时异常检测，并推送到 Telegram。

- **只报事实**：不判断方向，不给出入场、止损或目标价。
- **每个币用自己的基准**：以该币自身近 60 分钟的中位数为基准，不用全市场统一阈值，避免小币的正常波动淹没大币的真实异动。
- **双重门槛**：每项检测都同时要求「相对倍数」和「绝对门槛」，冷门币不会刷屏。

## 检测项

| # | 类型 | 触发条件（默认值） |
|---|---|---|
| 1 | 成交额异动 | 1 分钟成交额 ≥ 自身 60 分钟中位数 × 15，且 ≥ 300 万 USDT，且该分钟价格变动 ≥ 1% |
| 2 | 价格急变 | 1 分钟涨跌 ≥ 自身波动基准 × 12，且绝对值 ≥ 3% |
| 3 | 资金费率 | \|费率\| ≥ 0.20%，或 5 分钟内跳变 ≥ 0.05% |
| 4 | 持仓量突变 | 5 分钟 OI 变化 ≥ 8%，且 OI 名义价值 ≥ 1000 万 USDT |
| 5 | 突破 24h 高低 | 创 24 小时新高或新低，且伴随 3 倍量能、成交额 ≥ 100 万 USDT |

同一分钟同时触发 1 和 2 时合并为一条推送。

推送节流：同一币种同一类型 30 分钟内只推一次；全局每分钟最多 6 条；可设置免打扰时段。

## 数据源

- 单条 WebSocket 连接覆盖全市场：`!ticker@arr`（24h ticker，差分得到每分钟成交额与涨跌）和 `!markPrice@arr@1s`（标记价格与资金费率）。
- REST：`exchangeInfo` 和 `ticker/24hr` 用于筛选币种（24h 成交额 ≥ 300 万 USDT，每小时刷新）；`klines` 用于启动时预热基准；`openInterest` 用于轮询持仓量；`premiumIndex` 用于启动时预置资金费率。
- 30 秒收不到任何行情消息，就判定连接假死并自动重连。

> Binance 2026 年起把合约行情流迁到了 `/market` 路径。旧的 `/stream` 路径仍能握手，但不再推送数据。

## 部署（systemd）

需要 Python 3.9 及以上。

```bash
git clone https://github.com/jiemo9527/anomaly-radar.git /opt/anomaly-radar
cd /opt/anomaly-radar
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp radar.env.example radar.env && chmod 600 radar.env   # 填写 TG_BOT_TOKEN、TG_CHAT_ID
cp anomaly-radar.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now anomaly-radar
journalctl -u anomaly-radar -f
```

启动后会先推送一条「📡 异动雷达已启动」，此后每 30 分钟在日志里输出一次心跳。

## 配置

- **Telegram 凭据**：通过环境变量 `TG_BOT_TOKEN`、`TG_CHAT_ID` 提供，见 `radar.env.example`。未配置时只打印日志，不推送。
- **检测阈值、覆盖范围、节流、免打扰、时区**：都在 `radar.py` 顶部的「配置」区。
  - `EXCLUDE`：屏蔽指定币种。
  - `INCLUDE_TRADFI`：是否包含美股、港股、商品等 TradFi 永续合约。
  - `QUIET_START` / `QUIET_END`：免打扰时段。

## 免责声明

本项目只做数据监测，推送内容不构成任何投资建议。

## 许可证

[MIT](LICENSE)
