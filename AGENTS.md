# Development checks

Before every commit, run `make fmt`, stage the formatted files, and run
`make check`. These commands cover Go formatting, imports and lint across all
modules. Do not commit if a check fails.

Run `make install-hooks` once in each new checkout to install the pre-commit
check. If a hook already exists, preserve it and include `make check` in it.
Keep short development commands in the Makefile.
