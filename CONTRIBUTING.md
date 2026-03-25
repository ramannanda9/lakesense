# Contributing to lakesense

Thanks for your interest. Contributions are welcome — especially new storage
backends, plugins, and sketch integrations.

## Setup

```bash
git clone https://github.com/yourusername/lakesense
cd lakesense
pip install -e ".[dev]"
```

## Running tests

```bash
pytest                  # all tests
pytest tests/unit/      # unit tests only
pytest -x               # stop on first failure
```

## Code style

```bash
ruff check .            # lint
ruff format .           # format
mypy lakesense/       # type check
```

All three must pass before a PR is merged. CI enforces this.

## Adding a storage backend

1. Create `lakesense/storage/yourbackend.py`
2. Subclass `StorageBackend` from `lakesense.core.plugin`
3. Implement all five abstract methods
4. Add `yourbackend` to `[project.optional-dependencies]` in `pyproject.toml`
5. Add tests in `tests/unit/test_storage.py` following the existing pattern

See `lakesense/storage/parquet.py` as the reference implementation.

## Adding a plugin

1. Create `lakesense/plugins/yourplugin.py`
2. Subclass `SketchPlugin` from `lakesense.core.plugin`
3. Implement `should_run()` and `run()`
4. Export from `lakesense/plugins/__init__.py`
5. Add an example in `examples/`

See `lakesense/plugins/slack.py` as the reference implementation.

## PR checklist

- [ ] Tests pass (`pytest`)
- [ ] Lint passes (`ruff check .`)
- [ ] Type check passes (`mypy lakesense/`)
- [ ] New functionality has unit tests
- [ ] Docstring on public classes and functions
- [ ] Entry in `CHANGELOG.md` under `[Unreleased]`

## Reporting bugs

Open an issue with:
- lakesense version (`pip show lakesense`)
- Python version
- Minimal reproduction script
- Full traceback
