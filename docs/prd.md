# Product Requirements Document: XIRR UDAF for DataFusion

## 1. Objective

To provide data engineers and analysts with a highly performant and scalable User-Defined Aggregate Function (UDAF) for calculating the Extended Internal Rate of Return (XIRR) directly within the DataFusion query engine. This will enable efficient financial calculations on large-scale datasets (billions of records) stored in systems like Apache Iceberg without requiring data movement to external systems.

## 2. Target Audience

- **Data Engineers**: Building and maintaining data pipelines that involve financial calculations.
- **Data Analysts & Quants**: Running complex financial models on large datasets using SQL.

## 3. Functional Requirements

| ID | Requirement | Description |
|---|---|---|
| FR-01 | **UDAF Signature** | The function, named `xirr`, must accept two arguments: a column of `Float64` for cash flow amounts and a column of `Timestamp` for the corresponding dates. |
| FR-02 | **Aggregation** | The UDAF must aggregate these cash flows based on a `GROUP BY` clause (e.g., `user_id`). |
| FR-03 | **XIRR Calculation** | The function must accurately calculate the XIRR for each group's cash flows using the Newton-Raphson method. |
| FR-04 | **Return Value** | The function shall return a `Float64` representing the calculated XIRR. |
| FR-05 | **Error & Null Handling** | - If a group contains fewer than two cash flows, the function should return `NULL`.<br>- If the Newton-Raphson algorithm fails to converge, the function should return `NULL`.<br>- `NULL` values in the input columns should be ignored. |

## 4. Non-Functional Requirements

| ID | Requirement | Description |
|---|---|---|
| NFR-01 | **Performance** | The UDAF must be vectorized and designed for low-overhead execution. It should leverage zero-copy principles where possible by operating directly on Arrow memory buffers. |
| NFR-02 | **Scalability** | The implementation must be thread-safe (`Send + Sync`) to allow DataFusion's execution engine to process massive datasets in parallel across all available CPU cores. |
| NFR-03 | **Integration** | The UDAF must be implemented using the official `AggregateUDF` and `Accumulator` traits to ensure seamless integration and forward compatibility with DataFusion. |

## 5. Out of Scope

-   **Alternative IRR Calculation Methods**: This implementation will focus solely on the Newton-Raphson method for XIRR. Other methods like `MIRR` or standard `IRR` (for periodic cash flows) are not included.
-   **ADBC/Iceberg Connector**: While the UDAF is designed to work with data from these systems, this project does not include the implementation of the data connectors themselves.
