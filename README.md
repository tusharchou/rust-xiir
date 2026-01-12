# XIRR UDAF for DataFusion

This project provides a User-Defined Aggregate Function (UDAF) for calculating the Internal Rate of Return (IRR) for a series of cash flows with irregular intervals (XIRR) within the [Apache Arrow DataFusion](https://arrow.apache.org/datafusion/) query engine.

The implementation uses the Newton-Raphson method to find the root of the XIRR equation.

## Key Features

- **Vectorized & Thread-Safe**: Designed to run efficiently on multi-core processors.
- **DataFusion Integration**: Implements DataFusion's `AggregateUDF` and `Accumulator` traits for seamless integration.
- **Zero-Copy (where possible)**: Operates on Arrow arrays to minimize data copying.
- **Robust Error Handling**: Uses Rust's `Result` type to handle potential errors during execution.

## Getting Started

### Prerequisites

- [Rust and Cargo](https://www.rust-lang.org/tools/install)

### Building and Running

1.  **Clone the repository** (or download the source code).
2.  **Navigate to the project directory**.
3.  **Run the example**:
    ```bash
    cargo run
    ```

This will compile the project and run the example in `src/main.rs`, which:
1.  Initializes a DataFusion `SessionContext`.
2.  Registers the `xirr` UDAF.
3.  Creates a sample `RecordBatch` of transactions.
4.  Executes a SQL query to calculate the XIRR for each `user_id`.
5.  Prints the results to the console.

## Usage

The UDAF is registered with the name `xirr` and can be used in any DataFusion SQL query.

### Signature

```sql
xirr(amount, date)
```

- `amount`: A `Float64` column representing the cash flow amounts.
- `date`: A `Timestamp` column representing the date of each cash flow.

### Example Query

```sql
SELECT
    user_id,
    xirr(amount, date) AS xirr
FROM
    transactions
GROUP BY
    user_id;
```

## Implementation Details

The XIRR calculation is performed using the **Newton-Raphson method**, an iterative numerical method for finding the roots of a real-valued function.

1.  **XIRR Equation**: The function we are trying to solve is the Net Present Value (NPV) equation set to zero:
    ```
    NPV = Σ [ amount_i / (1 + rate)^((date_i - date_0) / 365) ] = 0
    ```
2.  **Accumulator (`XirrAccumulator`)**:
    - For each group (e.g., `user_id`), this accumulator collects all `(amount, date)` pairs into a `Vec<CashFlow>`.
    - The cash flows are kept sorted by date.
3.  **Evaluation (`evaluate`)**:
    - Once all cash flows for a group have been accumulated, the `evaluate` method is called.
    - It runs the Newton-Raphson algorithm on the collected cash flows to find the `rate`.
    - If a rate is found, it is returned as a `Float64`; otherwise, `NULL` is returned.
4.  **Parallelism (`merge_batch`)**: DataFusion's engine may process data in parallel partitions. The `merge_batch` function is responsible for combining the intermediate accumulator states from different partitions into a single state before the final evaluation.

## Performance Considerations

- **`Send + Sync`**: The UDAF and its accumulator are designed to be `Send + Sync`, allowing DataFusion's multi-threaded execution engine to safely process data partitions in parallel across all available CPU cores.
- **`Arc`**: `Arc` is used to share the UDAF definition and accumulator factory across threads efficiently, avoiding costly data cloning.
- **Partitioned Execution**: The `GROUP BY` clause naturally partitions the data. DataFusion processes these partitions in parallel, and the `merge_batch` function efficiently combines the results, following a map-reduce paradigm.
