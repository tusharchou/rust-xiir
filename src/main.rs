use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, ListArray, TimestampNanosecondArray, GenericListArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{create_udaf, Accumulator, AggregateUDF, Volatility};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use jiff::civil::Date;

/// Represents a cash flow with an amount and a date.
#[derive(Debug, Clone, Copy)]
struct CashFlow {
    amount: f64,
    date: Date,
}

/// XIRR Accumulator
#[derive(Debug, Default)]
struct XirrAccumulator {
    cash_flows: Vec<CashFlow>,
}

impl Accumulator for XirrAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let amounts: Vec<Option<f64>> = self.cash_flows.iter().map(|cf| Some(cf.amount)).collect();
        let dates: Vec<Option<i64>> = self
            .cash_flows
            .iter()
            .map(|cf| {
                let zdt = cf.date.to_zoned(jiff::tz::TimeZone::UTC).unwrap();
                Some(zdt.timestamp().as_second() * 1_000_000_000 + zdt.nanosecond() as i64)
            })
            .collect();

        let amount_array = Arc::new(Float64Array::from(amounts)) as ArrayRef;
        let date_array = Arc::new(TimestampNanosecondArray::from(dates)) as ArrayRef;

        let amount_list = Arc::new(GenericListArray::<i32>::new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::from_lengths([amount_array.len()]),
            amount_array,
            None,
        ));

        let date_list = Arc::new(GenericListArray::<i32>::new(
            Arc::new(Field::new("item", DataType::Timestamp(TimeUnit::Nanosecond, None), true)),
            OffsetBuffer::from_lengths([date_array.len()]),
            date_array,
            None,
        ));

        Ok(vec![
            ScalarValue::List(amount_list),
            ScalarValue::List(date_list),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "XIRR expects two arguments: amounts and dates".to_string(),
            ));
        }

        let amounts = values[0].as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Execution("Expected Float64 for amounts".to_string()))?;
        let dates = values[1].as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| DataFusionError::Execution("Expected Timestamp for dates".to_string()))?;

        for i in 0..amounts.len() {
            if amounts.is_valid(i) && dates.is_valid(i) {
                let ts = jiff::Timestamp::from_nanosecond(dates.value(i) as i128).unwrap();
                let zoned = jiff::Zoned::new(ts, jiff::tz::TimeZone::UTC);
                self.cash_flows.push(CashFlow {
                    amount: amounts.value(i),
                    date: zoned.date(),
                });
            }
        }
        self.cash_flows.sort_by_key(|cf| cf.date);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "XIRR merge expects two arguments, got {}",
                states.len()
            )));
        }

        let amounts_list = states[0].as_any().downcast_ref::<ListArray>().ok_or_else(|| DataFusionError::Internal("Could not downcast amounts in merge".to_string()))?;
        let dates_list = states[1].as_any().downcast_ref::<ListArray>().ok_or_else(|| DataFusionError::Internal("Could not downcast dates in merge".to_string()))?;

        for i in 0..amounts_list.len() {
            let amounts = amounts_list.value(i);
            let dates = dates_list.value(i);

            let amounts = amounts.as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Internal("Could not downcast amounts from list".to_string()))?;
            let dates = dates.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| DataFusionError::Internal("Could not downcast dates from list".to_string()))?;

            for j in 0..amounts.len() {
                if amounts.is_valid(j) && dates.is_valid(j) {
                    let ts = jiff::Timestamp::from_nanosecond(dates.value(j) as i128).unwrap();
                    let zoned = jiff::Zoned::new(ts, jiff::tz::TimeZone::UTC);
                    self.cash_flows.push(CashFlow {
                        amount: amounts.value(j),
                        date: zoned.date(),
                    });
                }
            }
        }
        self.cash_flows.sort_by_key(|cf| cf.date);
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.cash_flows.len() < 2 {
            return Ok(ScalarValue::Float64(None));
        }

        let payments = self
            .cash_flows
            .iter()
            .map(|cf| xirr::Payment {
                amount: cf.amount,
                date: cf.date,
            })
            .collect::<Vec<_>>();

        let rate = xirr::compute(&payments);

        match rate {
            Ok(rate) => Ok(ScalarValue::Float64(Some(rate))),
            Err(_) => Ok(ScalarValue::Float64(None)), // Or return an error
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.cash_flows.capacity() * std::mem::size_of::<CashFlow>()
            - std::mem::size_of::<Self>()
    }
}

/// Creates the XIRR AggregateUDF
fn create_xirr_udaf() -> AggregateUDF {
    let accumulator_factory: Arc<dyn Fn(AccumulatorArgs) -> Result<Box<dyn Accumulator>> + Send + Sync> =
        Arc::new(|_| Ok(Box::new(XirrAccumulator::default())));

    let state_ty = Arc::new(vec![
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ))),
    ]);

    create_udaf(
        "xirr",
        vec![
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        accumulator_factory,
        state_ty,
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
        NaiveDate::from_ymd_opt(2025, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap(),
        NaiveDate::from_ymd_opt(2025, 2, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap(),
        NaiveDate::from_ymd_opt(2026, 3, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap(),
        NaiveDate::from_ymd_opt(2027, 4, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap(),
        NaiveDate::from_ymd_opt(2028, 5, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_nanos_opt().unwrap(),
    ]));

    let batch = RecordBatch::try_from_iter(vec![
        ("user_id", user_ids as ArrayRef),
        ("amount", amounts as ArrayRef),
        ("date", dates as ArrayRef),
    ])?;

    ctx.register_batch("transactions", batch)?;

    // 4. Execute a SQL query that groups by user_id and calculates the XIRR
    let df = ctx
        .sql(
            "SELECT user_id, xirr(amount, date) AS xirr FROM transactions GROUP BY user_id",
        )
        .await?;

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
