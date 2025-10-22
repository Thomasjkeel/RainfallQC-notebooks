import polars as pl
import rainfallqc

def load_local_GSDR_data(gauge_id):
    data_path = f"../../data/GSDR/{gauge_id}.txt"
    # read in metadata of gauge
    GSDR_metadata = rainfallqc.utils.data_readers.read_gsdr_metadata(data_path)
    target_gauge_col = f"rain_{GSDR_metadata['original_units']}_{gauge_id}"

    # read in gauge data
    GSDR_data = pl.read_csv(
        data_path,
        skip_rows=20,
        schema_overrides={target_gauge_col: pl.Float64},
    )

    # add datetime column to data
    GSDR_data = rainfallqc.utils.data_readers.add_datetime_to_gsdr_data(
        GSDR_data, GSDR_metadata, multiplying_factor=24
    )
    GSDR_data = rainfallqc.utils.data_utils.replace_missing_vals_with_nan(
        GSDR_data, target_gauge_col=target_gauge_col, missing_val=int(GSDR_metadata["no_data_value"])
    )
    return GSDR_data.select(["time", target_gauge_col])


def load_local_GSDR_metadata(gauge_id):
    data_path = f"../../data/GSDR/{gauge_id}.txt"
    # read in metadata of gauge
    GSDR_metadata = rainfallqc.utils.data_readers.read_gsdr_metadata(data_path)
    return GSDR_metadata