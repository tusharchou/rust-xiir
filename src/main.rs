use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use chrono::prelude::*;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{create_udaf, Accumulator, AggregateUDF, Signature, Volatility};
use datafusion::physical_plan::expressions::format_state_name;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

/// Represents a cash flow with an amount and a date.
#[derive(Debug, Clone, Copy)]
struct CashFlow {
    amount: f64,
    date_ns: i64,
}

/// Calculates the net present value (NPV) of a series of cash flows for a given rate.
fn npv(rate: f64, cash_flows: &[CashFlow]) -> f64 {
    let first_date_ns = cash_flows[0].date_ns;
    cash_flows.iter().map(|cf| {
        let years = (cf.date_ns - first_date_ns) as f64 / (365.0 * 24.0 * 60.0 * 60.0 * 1_000_000_000.0);
        cf.amount / (1.0 + rate).powf(years)
    }).sum()
}

/// Calculates the derivative of the NPV with respect to the rate.
fn d_npv(rate: f64, cash_flows: &[CashFlow]) -> f64 {
    let first_date_ns = cash_flows[0].date_ns;
    cash_flows.iter().map(|cf| {
        let years = (cf.date_ns - first_date_ns) as f64 / (365.0 * 24.0 * 60.0 * 60.0 * 1_000_000_000.0);
        -years * cf.amount / (1.0 + rate).powf(years + 1.0)
    }).sum()
}


/// Calculates the XIRR using the Newton-Raphson method.
fn calculate_xirr(cash_flows: &[CashFlow]) -> Option<f64> {
    if cash_flows.len() < 2 {
        return None;
    }

    let mut guess = 0.1; // Initial guess
    for _ in 0..100 { // Max 100 iterations
        let npv_val = npv(guess, cash_flows);
        let d_npv_val = d_npv(guess, cash_flows);
        if d_npv_val.abs() < 1e-9 { // Avoid division by zero
            break;
        }
        let new_guess = guess - npv_val / d_npv_val;
        if (new_guess - guess).abs() < 1e-9 { // Convergence
            return Some(new_guess);
        }
        guess = new_guess;
    }
    None
}

/// XIRR Accumulator
#[derive(Debug, Default)]
struct XirrAccumulator {
    cash_flows: Vec<CashFlow>,
}

impl Accumulator for XirrAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let amounts = self.cash_flows.iter().map(|cf| ScalarValue::Float64(Some(cf.amount))).collect::<Vec<_>>();
        let dates = self.cash_flows.iter().map(|cf| ScalarValue::Int64(Some(cf.date_ns))).collect::<Vec<_>>();

        Ok(vec![
            ScalarValue::List(Some(amounts), Arc::new(Field::new("item", DataType::Float64, true))),
            ScalarValue::List(Some(dates), Arc::new(Field::new("item", DataType::Int64, true))),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution("XIRR expects two arguments: amounts and dates".to_string()));
        }

        let amounts = values[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Execution("Expected Float64 for amounts".to_string()))?;
        let dates = values[1].as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| DataFusionError::Execution("Expected Timestamp for dates".to_string()))?;

        for i in 0..amounts.len() {
            if amounts.is_valid(i) && dates.is_valid(i) {
                self.cash_flows.push(CashFlow {
                    amount: amounts.value(i),
                    date_ns: dates.value(i),
                });
            }
        }
        self.cash_flows.sort_by_key(|cf| cf.date_ns);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
             return Err(DataFusionError::Internal(format!(
                "XIRR merge expects two arguments, got {}",
                states.len()
            )));
        }
        let amounts_list = states[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Internal("Could not downcast amounts in merge".to_string()))?;
        let dates_list = states[1].as_any().downcast_ref::<Int64Array>().ok_or_else(|| DataFusionError::Internal("Could not downcast dates in merge".to_string()))?;

        for i in 0..amounts_list.len() {
             if amounts_list.is_valid(i) && dates_list.is_valid(i) {
                self.cash_flows.push(CashFlow {
                    amount: amounts_list.value(i),
                    date_ns: dates_list.value(i),
                });
            }
        }
        self.cash_flows.sort_by_key(|cf| cf.date_ns);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match calculate_xirr(&self.cash_flows) {
            Some(rate) => Ok(ScalarValue::Float64(Some(rate))),
            None => Ok(ScalarValue::Float64(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.cash_flows.capacity() * std::mem::size_of::<CashFlow>() - std::mem::size_of::<Self>()
    }
}

/// Creates the XIRR AggregateUDF
fn create_xirr_udaf() -> AggregateUDF {
    let accumulator_factory: Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync> =
        Arc::new(|| Ok(Box::new(XirrAccumulator::default())));

    create_udaf(
        "xirr",
        Signature::exact(vec![DataType::Float64, DataType::Timestamp(TimeUnit::Nanosecond, None)], Volatility::Immutable),
        Arc::new(|_| Ok(Arc::new(DataType::Float64))),
        accumulator_factory,
        Arc::new(vec![
            Arc::new(Field::new("amounts", DataType::List(Arc::new(Field::new("item", DataType::Float64, true))), true)),
            Arc::new(Field::new("dates", DataType::List(Arc::new(Field::new("item", DataType::Int64, true))), true)),
        ])
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize a SessionContext
    let ctx = SessionContext::new();

    // 2. Register the custom XIRR UDAF
    ctx.register_udaf(create_xirr_udaf());

    // 3. Create a dummy DataFrame to simulate the data from an Iceberg table
    let user_ids = Arc::new(Float64Array::from(vec![1.0, 2.0, 1.0, 2.0, 1.0]));
    let amounts = Arc::new(Float64Array::from(vec![-100.0, -120.0, 20.0, 40.0, 110.0]));
    let dates = Arc::new(TimestampNanosecondArray::from(vec![
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_nanos(),
        NaiveDate::from_ymd_opt(2025, 2, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_nanos(),
        NaiveDate::from_ymd_opt(2026, 3, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_nanos(),
        NaiveDate::from_ymd_opt(2027, 4, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_nanos(),
        NaiveDate::from_ymd_opt(2028, 5, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_nanos(),
    ]));

    let batch = RecordBatch::try_from_iter(vec![
        ("user_id", user_ids as ArrayRef),
        ("amount", amounts as ArrayRef),
        ("date", dates as ArrayRef),
    ])?;

    ctx.register_batch("transactions", batch)?;

    // 4. Execute a SQL query that groups by user_id and calculates the XIRR
    let df = ctx.sql("SELECT user_id, xirr(amount, date) AS xirr FROM transactions GROUP BY user_id").await?;

    // 5. Print the results
    df.show().await?;

    println!("XIRR UDAF implementation and execution complete.");
    println!("\n--- Performance Optimization Explanation ---");
    println!("To ensure this computation scales across all available CPU cores, we leverage several key Rust and DataFusion concepts:");
    println!("1. Send + Sync: The `XirrAccumulator` factory is wrapped in an `Arc`, making it shareable across threads. The `Send` and `Sync` traits, automatically derived for our accumulator, guarantee that it can be safely sent to and shared between threads. DataFusion's execution engine uses a pool of threads (Tokio tasks) to process partitions of data in parallel. Each thread gets its own `XirrAccumulator` instance for the partitions it handles.");
    println!("2. Arc<...>: `Arc` (Atomically Referenced Counter) is used to share the UDAF definition and accumulator factory across threads without needing to clone the underlying data. This is crucial for low-overhead parallelization.");
    println!("3. Partitioned Execution: DataFusion automatically partitions the data by `user_id` (the GROUP BY key). Each partition is processed independently. The `merge_batch` method on the `XirrAccumulator` is the key to combining the intermediate results from different partitions for the same user if they happen to be processed on different threads. This is a classic map-reduce style aggregation.");
    println!("4. Zero-Copy: The UDAF operates directly on Arrow `ArrayRef`s. These are effectively zero-copy views into the underlying memory buffers. We only pay the cost of converting the data into our `CashFlow` struct within the accumulator. For a production system, you might further optimize this by keeping the data in columnar format within the accumulator to reduce this overhead.");

    Ok(())
}
