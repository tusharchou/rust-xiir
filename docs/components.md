# Project Components

This document details the main components of the XIRR UDAF project.

## 1. `Cargo.toml`

-   **Purpose**: The manifest file for the Rust project.
-   **Details**: It defines project metadata and lists all necessary dependencies, such as `datafusion`, `arrow`, and `tokio`. This file is the entry point for building and running the project via `cargo`.

## 2. `README.md`

-   **Purpose**: The main documentation for the project.
-   **Details**: Provides a high-level overview of the project's purpose, features, and usage. It includes instructions for getting started, a SQL usage example, and a summary of the implementation and performance characteristics.

## 3. `src/main.rs`

-   **Purpose**: The main source file containing all the logic for the UDAF and a runnable example.

-   **Key Sections**:
    -   **`main` function**: The entry point of the executable. It serves as an orchestration and testing module that:
        1.  Initializes a DataFusion `SessionContext`.
        2.  Registers the `xirr` UDAF.
        3.  Creates an in-memory `RecordBatch` to simulate data from a table.
        4.  Executes a SQL query using the UDAF.
        5.  Prints the results and performance explanations.
    -   **`XirrAccumulator` struct**: A stateful struct that implements the `datafusion::logical_expr::Accumulator` trait. It is the core of the UDAF, responsible for collecting and storing the cash flows for each group during the aggregation process.
    -   **`calculate_xirr` function**: A helper function that contains the pure mathematical logic for calculating the XIRR using the Newton-Raphson method. It is called by the `XirrAccumulator`'s `evaluate` method.
    -   **`create_xirr_udaf` function**: A factory function that constructs and returns the `AggregateUDF`. It defines the UDAF's signature (name, input types, return type) and provides the `XirrAccumulator` factory to the DataFusion query planner.

## 4. `docs/`

-   **Purpose**: A directory containing detailed project documentation.
-   **Contents**:
    -   **`prd.md`**: The Product Requirements Document, outlining the goals and requirements for the UDAF.
    -   **`components.md`**: This file, which provides a breakdown of the project's structure and components.
