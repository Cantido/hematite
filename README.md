# Hematite

Hematite is an event-store database for events following the [CloudEvents](https://cloudevents.io) specification.

Storing events for an event-sourced system allows for some serious database optimizations:

- Events can never be changed after they are sent
- Events are strictly ordered by the order they arrived
- Events are frequently queried by their position in history

Hematite is built with these optimizations and so and read and write events very fast.

## Usage

This is an experimental project, don't use it.


