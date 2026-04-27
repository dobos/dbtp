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
