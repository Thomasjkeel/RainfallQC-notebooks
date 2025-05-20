import polars as pl
import rainfallqc

def load_local_gdsr_data(gauge_id):
    data_path = f"../../data/GDSR/{gauge_id}.txt"
    # read in metadata of gauge
    gdsr_metadata = rainfallqc.utils.data_readers.read_gdsr_metadata(data_path)
    rain_col = f"rain_{gdsr_metadata['original_units']}"

    # read in gauge data
    gdsr_data = pl.read_csv(
        data_path,
        skip_rows=20,
        schema_overrides={rain_col: pl.Float64},
    )

    # add datetime column to data
    gdsr_data = rainfallqc.utils.data_readers.add_datetime_to_gdsr_data(
        gdsr_data, gdsr_metadata, multiplying_factor=24
    )
    gdsr_data = rainfallqc.utils.data_utils.replace_missing_vals_with_nan(
        gdsr_data, rain_col=rain_col, missing_val=int(gdSsr_metadata["no_data_value"])
    )
    return gdsr_data.select(["time", rain_col])


def load_local_gdsr_metadata(gauge_id):
    data_path = f"../../data/GDSR/{gauge_id}.txt"
    # read in metadata of gauge
    gdsr_metadata = rainfallqc.utils.data_readers.read_gdsr_metadata(data_path)
    return gdsr_metadata