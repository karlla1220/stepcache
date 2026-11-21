# stepcache

Content-addressed cache for multi-step processing pipelines, with early cutoff.

Each step's result is keyed by the content hashes of its inputs and the identity of
the step (its code, tools and settings). When a step changes, only the items whose
inputs or step identity changed run again, and items whose output comes out
byte-identical stop the re-run there: later steps keep their cached results.

Status: design, no code yet. The design is in [docs/design.md](docs/design.md) and the build order in
[docs/plan.md](docs/plan.md) (both in Korean).

## Development

Run the tests with [uv](https://docs.astral.sh/uv/):

```
uv run pytest
```

## License

Apache-2.0. See [LICENSE](LICENSE).
