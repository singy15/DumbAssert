# DumbAssert

DumbAssert, dumb database testing utility for C#


## About

DumbAssert is database testing utility as lesser/badder DBUnit alternative on C#.


## Feature

- Load test data from CSV files.

- Comparison of expected value prepared in CSV file and actual value in database.


## Usage

1. Add DumbAssert.cs to your project.

2. Prepare test data.

3. `DumbAssert.Prepare({testId})` to load test data.

4. Do some operation to database.

5. `DumbAssert.Assert({testId})` to assert database.


