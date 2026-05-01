# Database Transaction Processing Exercise Generator

This mostly AI-generated library can generate exercises for DB transaction processing which include:

* Transaction schedules
    * Generate conflict-equivalent schedules
    * Generate a mix of conflict-equivalent and non-conflict-equivalent schedules
* Locking schedules
    * Generate schedules with 2PL
    * Generate schedules which end up in deadlock under 2PL

## Running the code

From bash:

    $ export PYTHONPATH="./python"
    $ ./bin/dbtp ...

From PS:

    > $env:PYTHONPATH="./python"
    > .\bin\dbtp.ps1 ...

# Conflict equivalence exercises

## Generic command-line parameters

General parameters:

* `--seed`: random seed for reproducible generation
* `--graph`: print the conflict graph of the schedules
* `--latex`: whether to output the schedule in LaTeX format

Parameters controlling schedule generation:

* `--num-transactions`: number of transactions in the schedule
* `--num-operations`: number of operations in the schedule
* `--must-read`, `--no-must-read`: must read every data item before writing it
* `--must-write`, `--no-must-write`: must write every data item before reading it
* `--serializable`, `--no-serializable`: whether the generated schedule must be serializable
* `--allow-two-node-cycles`, `--no-allow-two-node-cycles`: whether cyclic precedence graphs may use trivial two-transaction cycles (default: disabled)
* `--random-item-reuse`: when set, data items may be randomly reused across edges (increasing conflicts)
* `--new-item-probability`: probability of introducing a new data item when `--random-item-reuse` is active (0.0–1.0, default 0.5)
* `--num-non-conflicting-operations`: number of extra non-conflicting operations inserted at random positions after schedule generation (default 0); each is a validated `READ` on an already-used data item label, and only insertions that preserve the original conflict graph are kept

## Generate conflict-equivalent schedules

Run `dbtp conf-eq` to generate conflict-equivalent schedules. The problem-specific parameters are:

* `--num-schedules`: number of schedules to generate

Example:

    $ ./bin/dbtp conf-eq --num-schedules 3 --num-transactions 4 --num-operations 4 --seed 42

## Generate a mix of conflict-equivalent and non-conflict-equivalent schedules

Run `dbtp conf-eq-mix` to generate a mix of conflict-equivalent and non-conflict-equivalent schedules. The problem-specific parameters are:

* `--num-equivalent`: Number of conflict-equivalent schedules to generate
* `--num-non-equivalent`: Number of non-conflict-equivalent schedules to generate

Example:

    $ ./bin/dbtp conf-eq-mix --num-transactions 4 --num-operations 4 --seed 42

# Dead-locking schedules

Run `dbtp deadlock` to generate strict-2PL locking schedules whose wait-for graph either contains a cycle (deadlock) or remains acyclic (no deadlock).

Problem-specific parameters:

* `--num-transactions`: number of transactions in the schedule
* `--num-operations`: number of wait-for edges to generate
* `--seed`: random seed for reproducible generation
* `--deadlocking`: force the generated schedule to deadlock (default)
* `--non-deadlocking`: force the generated schedule to avoid deadlock
* `--graph`: print the wait-for graph derived from the schedule
* `--graph-after-ops`: print the wait-for graph after the first `N` operations instead of after the full schedule
* `--latex`: output the schedule and the wait-for graph in LaTeX/TikZ format

The generator guarantees that every produced schedule is legal and satisfies strict 2PL. When `--deadlocking` is selected, the resulting wait-for graph contains a cycle; when `--non-deadlocking` is selected, the wait-for graph is acyclic.

Example: generate a deadlocking schedule and print the full wait-for graph.

    $ ./bin/dbtp deadlock --num-transactions 4 --num-operations 4 --seed 13 --graph

Example: generate a non-deadlocking schedule and inspect the graph after the first 6 operations.

    $ ./bin/dbtp deadlock --non-deadlocking --num-transactions 4 --num-operations 4 --seed 19 --graph --graph-after-ops 6



