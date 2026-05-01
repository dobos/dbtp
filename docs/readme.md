# Database Transaction Processing Exercise Generator

This mostly AI-generated library can generate exercises for DB transaction processing which include:

* Transaction schedules
  * Generate conflict-equivalent schedules
* Locking schedules


## Running the code

From bash:

    $ ./bin/dbtp ...

From PS:

    > $env:PYTHONPATH="./python"
    > .\bin\dbtp.ps1 ...

# Conflict-equivalent schedule generation

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
* `--num-non-conflicting-operations`: number of extra non-conflicting operations inserted at random positions after schedule generation (default 0); each uses a fresh data item not present in the original schedule, so no new conflict-graph edges are introduced

## Generate conflict-equivalent schedules

Run `dbtp conf-eq` to generate conflict-equivalent schedules. The problem-specific parameters are:

* `--num-schedules`: number of schedules to generate

Example:

    $ ./bin/dbtp conf-eq --num-schedules 3 --num-transactions 3 --num-operations 4 --seed 42

## Generate a mix of conflict-equivalent and non-conflict-equivalent schedules

Run `dbtp conf-eq-mix` to generate a mix of conflict-equivalent and non-conflict-equivalent schedules. The problem-specific parameters are:

* `--num-equivalent`: Number of conflict-equivalent schedules to generate
* `--num-non-equivalent`: Number of non-conflict-equivalent schedules to generate

Example:

    $ ./bin/dbtp conf-eq-mix --num-transactions 3 --num-operations 4 --seed 42
