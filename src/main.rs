use polars::frame::DataFrame;
use polars::prelude::{
    col, lit, CsvReader, DataType, IntoLazy, LazyFrame, ParquetCompression, ParquetWriter,
    SerReader,
};
use std::env;
use std::fs::File;
use tracing::{info, Level};
use tracing_subscriber::registry::Data;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    configure_tracing();
    configure_polars_env();
    let df = create_df_from_csv("data/worldcities.csv", false);
    let mut df = modify_dataframe(df);
    df_to_parquet(&mut df, "data/worldcities.parquet");
    let df = create_df_from_parquet("data/worldcities.parquet");
    info!("{}", df);
}

fn configure_tracing() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn configure_polars_env() {
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1");
    env::set_var("POLARS_FMT_MAX_COLS", "100");
    env::set_var("POLARS_FMT_MAX_ROWS", "50");
    env::set_var("POLARS_FMT_STR_LEN", "100");
}

fn create_df_from_csv(file_path: &str, has_header: bool) -> DataFrame {
    CsvReader::from_path(file_path)
        .unwrap()
        .has_header(has_header)
        .finish()
        .expect("could not create data frame from given cvs file")
}

fn create_df_from_parquet(file_path: &str) -> DataFrame {
    LazyFrame::scan_parquet(file_path, Default::default())
        .unwrap()
        .collect()
        .expect("could not create data frame from given cvs file")
}

fn modify_dataframe(data_frame: DataFrame) -> DataFrame {
    data_frame
        .lazy()
        .filter(col("column_2").neq(lit("city_ascii")))
        .with_column(col("column_3").cast(DataType::Float64))
        .with_column(col("column_4").cast(DataType::Float64))
        .with_column(col("column_10").cast(DataType::Int64))
        .with_column(col("column_11").cast(DataType::Int64))
        .rename(
            [
                "column_1",
                "column_3",
                "column_4",
                "column_5",
                "column_6",
                "column_7",
                "column_8",
                "column_9",
                "column_10",
                "column_11",
            ],
            [
                "city",
                "lat",
                "lng",
                "country",
                "iso2",
                "iso3",
                "admin_name",
                "capital",
                "population",
                "id",
            ],
        )
        .select([
            col("city"),
            col("lat"),
            col("lng"),
            col("country"),
            col("iso2"),
            col("iso3"),
            col("admin_name"),
            col("capital"),
            col("population"),
            col("id"),
        ])
        .collect()
        .unwrap()
}

fn df_to_parquet(data_frame: &mut DataFrame, target_path: &str) {
    let file = File::create(target_path).expect("Could not create a file for the given path");
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Lz4Raw)
        .with_statistics(false)
        .finish(data_frame)
        .expect("Could not write dataframe into file");
}
