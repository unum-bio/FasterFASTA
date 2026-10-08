# Git Hooks

`pre-commit` and `commit-msg` are the rulebook — each check carries its own rationale as a comment directly above it.

## Activation

Not automated.
Cargo's only pre-build hook is `build.rs`, and adding a build script to a crate that has none — just to mutate Git config as a side effect of `cargo build` — costs more than the one line it saves:

```sh
git config core.hooksPath .githooks
```

Note that `core.hooksPath` and `pre-commit install` are mutually exclusive — Git ignores `.git/hooks` entirely once the path is redirected.

## What They Check

`commit-msg` requires a `<Verb>: <Capitalized summary>` subject with one of the four sanctioned verbs, and rejects assistant attribution trailers and session links.
Merge and revert subjects pass through, since Git writes them.

`pre-commit` runs the style checks over each staged text file, then the toolchain gates.
The style checks cover banner separators, `// region:` balance, module declarations outside the crate root, banned vocabulary, the retired crate spelling, one `#[cfg(test)] mod tests` per source, backward-looking comments, abbreviated identifiers, `std::sync::Mutex` under `src/`, and Markdown emphasis and sentence-per-line.
`.githooks/` itself is exempt from every content scan, because a hook has to spell out the patterns it bans.

The toolchain gates run when a `.rs` file or `Cargo.toml` is staged, and are skipped when there is no `Cargo.toml` or no `cargo` on `PATH`.
Staged lockfiles are validated with `cargo metadata --locked`.
Cargo checks use `--locked` so validation cannot refresh the committed dependency set.
A cargo that fails to run at all is reported as a toolchain fault rather than as a formatting or lint violation.

## Selftest

```sh
.githooks/selftest
```

Every check gets a fixture that must be rejected, and the hooks get fixtures that must pass untouched.
A rejection counts only when exactly one check fired, which makes this a false-positive test as well.
Fixtures are built outside the repository; override the location with `FASTERFASTA_SELFTEST_DIR`.

## Portability

Bash 3.2 and a POSIX awk are enough, so stock macOS and Git for Windows both run these unchanged.
The `* text=auto eol=lf` rule in `.gitattributes` is load-bearing on Windows: a CRLF checkout gives the shebang a trailing carriage return, and every hook then exits 127.

## Bypassing

```sh
git commit --no-verify
```

There is no in-band escape comment, deliberately.
If a rule needs to bend for a legitimate one-off, edit the check here and document why in the comment above it.
