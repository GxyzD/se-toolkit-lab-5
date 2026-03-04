# Quality assurance

<h2>Table of contents</h2>

- [What is quality assurance](#what-is-quality-assurance)
- [Assertion](#assertion)
- [Static analysis](#static-analysis)
  - [Static analysis in this project](#static-analysis-in-this-project)
- [Static analysis techniques](#static-analysis-techniques)
  - [Formatting](#formatting)
  - [Linting](#linting)
  - [Type checking](#type-checking)
  - [Model checking](#model-checking)
- [Dynamic analysis](#dynamic-analysis)
  - [Dynamic analysis in this project](#dynamic-analysis-in-this-project)
- [Dynamic analysis techniques](#dynamic-analysis-techniques)
  - [Testing](#testing)
- [Test types](#test-types)
  - [Unit test](#unit-test)
  - [End-to-end test](#end-to-end-test)
- [Boundary value analysis](#boundary-value-analysis)

## What is quality assurance

Quality assurance is the practice of verifying that software works correctly. It combines two approaches: [static analysis](#static-analysis), which inspects code without running it, and [dynamic analysis](#dynamic-analysis), which executes code with specific inputs and checks the outputs.

Together, these techniques catch bugs early, prevent regressions when code changes, and document the intended behavior of a program.

Examples:

- [`poe check`](./pyproject-toml.md#poe-check) — run all static analysis tools.
- [`poe test`](./pyproject-toml.md#poe-test) — run all dynamic analysis tests.

## Assertion

An assertion is a statement that checks whether a given condition is true. If the condition is false, the assertion fails and raises an error, stopping the test immediately.

Assertions are the primary mechanism for verifying expected behavior in tests — each test typically ends with one or more assertions that confirm the code produced the right result.

Examples:

- [The `assert` statement in `Python`](./python.md#the-assert-statement)

## Static analysis

Static analysis checks code for errors without running it. It can detect type errors, undefined variables, and style issues.

### Static analysis in this project

Static analysis is run with [`poe check`](./pyproject-toml.md#poe-check), which executes several tools in sequence.

In the editor, [`Pylance`](./python.md#pylance) provides real-time static analysis as you type.

## Static analysis techniques

### Formatting

### Linting

### Type checking

### Model checking

## Dynamic analysis

Dynamic analysis checks code behavior by executing it. Errors are only detected when the relevant code path actually runs.

Examples:

- [Testing](#testing)

### Dynamic analysis in this project

Dynamic analysis is run with [`poe test`](./pyproject-toml.md#poe-test), which executes [unit tests](#unit-test) followed by [end-to-end tests](#end-to-end-test) using [`pytest`](./python.md#pytest).

## Dynamic analysis techniques

### Testing

<!-- TODO -->

<!-- TODO a couple of other techniques -->

## Test types

<!-- TODO -->

### Unit test

A unit test verifies that an individual function or module works correctly in isolation. Unit tests are fast because they don't depend on external services like databases or network connections.

In this project, unit tests are located in `backend/tests/unit/` and run with [`poe test-unit`](./pyproject-toml.md#poe-test-unit).

Examples:

- [Testing in `Python`](./python.md#testing)

### End-to-end test

An end-to-end (E2E) test verifies that the full system works correctly by sending real [`HTTP` requests](./http.md#http-request) to the deployed application and checking the responses. E2E tests are slower than [unit tests](#unit-test) because they depend on running services.

In this project, E2E tests are located in `backend/tests/e2e/` and run with [`poe test-e2e`](./pyproject-toml.md#poe-test-e2e).

## Boundary value analysis

Boundary value analysis is a testing technique that focuses on the edges of input ranges — values like `0`, `1`, an empty list, or the maximum allowed size. Bugs often occur at these boundaries because of off-by-one errors, missing edge-case handling, or incorrect comparison operators.

When writing tests, check both sides of each boundary: the value just inside the valid range and the value just outside it.
