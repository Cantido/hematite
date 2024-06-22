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

## License

Copyright © 2024 Rosa Richter

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
