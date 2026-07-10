# gh-aw (Agentic Workflows)

This project uses `gh-aw` to author agentic GitHub Actions from simple Markdown prompts. The configured engine is Gemini and the workflows live in `.github/aw/` and `.github/workflows/`.

## Quick usage

- Install extension:

```bash
gh extension install github/gh-aw
```

- After editing an `.md` workflow prompt, compile the locked workflow:

```bash
gh aw compile
```

- Commit both the `.md` source and the generated `.lock.yml` file.

- Run manually:

```bash
gh aw run <workflow-name>
```

## Current agentic workflow

- `weekly-doc-updater` — runs every Monday and opens a PR to keep the docs in sync with merged code changes. It requires `GEMINI_API_KEY` for the engine.

## Notes

- Prompt-only edits (no frontmatter changes) may not require recompilation, but if frontmatter or execution settings change, run `gh aw compile`.
- Secrets (API keys) used by agents must be stored in the repository or org secrets and referenced securely.
