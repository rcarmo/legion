# Telegram bot

Maps each Telegram chat to one durable Legion session using Picoclaw-compatible channel messages.

```sh
export TELEGRAM_BOT_TOKEN=…
bun examples/telegram-bot/index.ts
```

The token belongs in the process environment. Model-provider credentials belong in the Legion service environment. The adapter uses bounded Telegram long polling and correlated replies.
