# Hematite

Hematite is an event-store database for events following the [CloudEvents](https://cloudevents.io) specification.

Event-sourced systems have the following characteristics which allow for serious database optimizations:

- Events can never be changed after they are sent
- Events are strictly ordered by the order they arrived
- Events are frequently queried by their position in history

Hematite is built with these optimizations and so can read and write events very fast.
Run the benchmarks to see for yourself:

```console
$ cargo bench
```

## Usage

This is an experimental project, don't use it.


