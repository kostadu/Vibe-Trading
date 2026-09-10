# OKX Live ops (Vibe 0.1.15)

## Runtime
- Branch: `upgrade/v0.1.15` (+ local allowlist patches, uncommitted)
- Package: `vibe-trading-ai==0.1.15` (editable)
- LLM: DeepSeek `deepseek-v4-pro` + `VIBE_TRADING_DEEPSEEK_ADAPTER=openai-compatible`
- Service: `systemctl --user status vibe-okx-liverunner`
- Tick: 5 min · Mandate: BTC/ETH-USDC · full equity exposure · 12 trades/day
- Daily loss halt: cron `*/15` + proxy in script (`daily_loss_cap_usd` in policy)

## Do not
- `git checkout main` without re-applying allowlist patch (main is still 0.1.13 tip)
- Remove `openai-compatible` adapter (native path hits DeepSeek `reasoning_content` 400)
- Run OKX without proxy whitelist (`100.64.0.5:8888`)

## Backup
`~/.vibe-trading/upgrade-backup-20260910T104400Z`
