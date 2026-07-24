# escapement — Agent Notes

## Blackwall custody

This repo is under blackwall custody (initialized 2026-07-24).

- Custody store: `.blackwall/` (gitignored, local)
- Config: `blackwall.toml` (provider=gateway, model=llm/glm-5.2, verify=cargo fmt/clippy/test)
- Task library: `tasks/` (one task per Linear ticket)
- SOP: `~/.agents/sops/blackwall-orchestration.md`

### Running agent work

```bash
blackwall run start --task <name> --provider gateway
blackwall run show --latest
blackwall changeset --latest
blackwall settle <hash> apply <world>
blackwall reconcile <hash> --branch <branch>
blackwall pr <hash>
```

### Lessons

- 2026-07-24: Blackwall initialized at commit `4a80c74` (federation standards).
  Provider is `gateway` (LiteLLM tailnet). The root world snapshot is the
  federation commit.
