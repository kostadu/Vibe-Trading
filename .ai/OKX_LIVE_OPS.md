# OKX Live ops (Vibe 0.1.15) — low-churn

## Runtime
- Branch: `upgrade/v0.1.15`
- Package: `vibe-trading-ai==0.1.15` (editable)
- LLM: DeepSeek `deepseek-v4-pro` + `VIBE_TRADING_DEEPSEEK_ADAPTER=openai-compatible`
- Service: `systemctl --user status vibe-okx-liverunner`
- Tick: **30 min** · max-iter 14
- Mandate: BTC/ETH-USDC · **≤70% exposure** · order ≤$150 · **3 trades/day**
- Daily loss halt: cron `*/15` · cap **$50**
- Prompt bias: DEFAULT HOLD; rotate only if ≥~2% RS gap (tool-verified)

## Do not
- Push exposure to 100% of equity
- Remove `openai-compatible` adapter
- Call OKX without whitelist proxy `100.64.0.5:8888`

## Backup
`~/.vibe-trading/upgrade-backup-20260910T104400Z`
