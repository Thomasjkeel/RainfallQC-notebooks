import inspect
from rainfallqc import gauge_checks, comparison_checks, timeseries_checks, neighbourhood_checks

ALL_QC_METHODS = {
    "QC1": {"function": gauge_checks.check_years_where_nth_percentile_is_zero},
    "QC2": {"function": gauge_checks.check_years_where_annual_mean_k_top_rows_are_zero},
    "QC3": {"function": gauge_checks.check_temporal_bias},
    "QC4": {"function": gauge_checks.check_temporal_bias},
    "QC5": {"function": gauge_checks.check_intermittency},
    "QC6": {"function": gauge_checks.check_breakpoints},
    "QC7": {"function": gauge_checks.check_min_val_change},
    "QC8": {"function": comparison_checks.check_annual_exceedance_etccdi_r99p},
    "QC9": {"function": comparison_checks.check_annual_exceedance_etccdi_prcptot},
    "QC10": {"function": comparison_checks.check_exceedance_of_rainfall_world_record},
    "QC11": {"function": comparison_checks.check_annual_exceedance_etccdi_rx1day},
    "QC12": {"function": timeseries_checks.check_dry_period_cdd},
    "QC13": {"function": timeseries_checks.check_daily_accumulations},
    "QC14": {"function": timeseries_checks.check_monthly_accumulations},
    "QC15": {"function": timeseries_checks.check_streaks},
    "QC16": {"function": neighbourhood_checks.check_wet_neighbours},
 }


def run_example_qc_framework(data, qc_to_run, kwargs_map):
    result = {}
    shared_kwargs = kwargs_map.get("shared", {})

    for qc in qc_to_run:
        func = ALL_QC_METHODS[qc]["function"]
        specific_kwargs = kwargs_map.get(qc, {})
        combined_kwargs = {**shared_kwargs, **specific_kwargs}

        # Filter kwargs to only those the function accepts
        sig = inspect.signature(func)
        accepted_keys = set(sig.parameters.keys())
        filtered_kwargs = {
            k: v for k, v in combined_kwargs.items()
            if k in accepted_keys or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        }

        result[qc] = func(data, **filtered_kwargs)

    return result