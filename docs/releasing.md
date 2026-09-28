# Releasing rpckit

The current development version is 0.10.0. Until a release is cut, its
changelog heading stays `Unreleased`. Never assign a release date or tag to an
unfinished version.

## Compatibility during 0.x

Minor releases may change public APIs and generated client layouts. Record each
breaking change in `CHANGELOG.md` and add concrete migration steps. Consumers
regenerate their Python and TypeScript clients from the new contract when they
upgrade. Renamed names do not get compatibility aliases, and generated clients
do not add a separate version handshake.

## Release checklist

1. Confirm `pyproject.toml`, `rpckit.__version__`, and the changelog heading
   name the same version. Run `uv lock` after changing package metadata.
2. Regenerate the committed examples with
   `uv run rpckit generate --config examples/generated_clients/rpcgen.toml`.
   Then run the same command with `--check`.
3. Run `uv run ruff check .`, `uv run ruff format --check .`,
   `uv run python scripts/verify_types.py`, and `uv run pytest`.
4. Add migration steps for the release and replace `Unreleased` in the
   changelog heading with the release date (`YYYY-MM-DD`). Build with `uv build`
   and inspect the wheel and source archive.
5. Commit the release, tag that commit `v<version>`, and publish the matching
   artifacts. Push the commit and tag together. Start the next version with a
   new `Unreleased` heading.

The release gate uses this repository's tests and generated-client checks.
