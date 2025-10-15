import pandas as pd

# Available data files
csv_population = "/workspaces/orchestration-workshop-tutorial/data/population-with-un-projections.csv"
csv_renewable_share = "/workspaces/orchestration-workshop-tutorial/data/renewable-share-energy.csv"
csv_energy_consumption = "/workspaces/orchestration-workshop-tutorial/data/primary-energy-cons.csv"
csv_regional_grouping = "/workspaces/orchestration-workshop-tutorial/data/regional-grouping.csv"


def _prepare_population(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the population column names.
    """
    population = df.rename(
        columns={
            "population__sex_all__age_all__variant_estimates": "population",
            "Entity": "entity",
            "Code": "entity_code",
            "Year": "year",
        }
    )
    return population[["entity", "entity_code", "year", "population"]]


def _prepare_renewable_energy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the renewable energy contributions column names.
    """
    renewable_coverage_df = df.rename(
        columns={
            "renewables__pct_equivalent_primary_energy": "renewable_energy_pct",
            "Entity": "entity",
            "Code": "entity_code",
            "Year": "year",
        }
    ).assign(renewable_energy_pct=lambda x: x["renewable_energy_pct"] / 100)
    return renewable_coverage_df[
        ["entity", "entity_code", "year", "renewable_energy_pct"]
    ]


def _prepare_energy_consumption(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the energy consumption column names.
    Standardize units to GWH.
    """
    energy_consumption_df = df.rename(
        columns={
            "primary_energy_consumption__twh": "energy_consumption_twh",
            "Entity": "entity",
            "Code": "entity_code",
            "Year": "year",
        }
    ).assign(energy_consumption_gwh=lambda x: x["energy_consumption_twh"] * 1000)
    return energy_consumption_df[
        ["entity", "entity_code", "year", "energy_consumption_gwh"]
    ]


def _get_df_from_csv(csv_file: str) -> pd.DataFrame:
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} rows from {csv_file}")
    return df


def _define_fossil_and_renewable_coverage(
    energy_consumption_df: pd.DataFrame, renewable_energy_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Estimate the fossil and renewable energy consumption in GWH.
    """
    return energy_consumption_df.merge(
        renewable_energy_df, how="left", on=["entity", "entity_code", "year"]
    ).assign(
        renewable_energy_pct=lambda x: x["renewable_energy_pct"].fillna(0),
        fossil_energy_pct=lambda x: 1 - x["renewable_energy_pct"],
        renewable_energy_consumption_gwh=lambda x: x["energy_consumption_gwh"]
        * x["renewable_energy_pct"],
        fossil_energy_consumption_gwh=lambda x: x["energy_consumption_gwh"]
        * x["fossil_energy_pct"],
    )


def _add_population_data_to_energy_breakdown(
    energy_breakdown_df: pd.DataFrame, population_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add population metadata to energy breakdowns.
    """
    return energy_breakdown_df.merge(
        population_df, how="left", on=["entity", "entity_code", "year"]
    )


def _create_regional_rollups(
    energy_breakdown_df: pd.DataFrame, regional_grouping_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Create aggregates across entities using the provided mapping.
    Treat the regions as entities on their own regard.
    """
    entities_of_interest = energy_breakdown_df.merge(
        regional_grouping_df, on="entity_code"
    )

    return (
        entities_of_interest.groupby(
            [
                "region_entity_code",
                "region_name",
                "year",
            ],
            as_index=False,
        )
        .agg(
            {
                "population": "sum",
                "energy_consumption_gwh": "sum",
                "renewable_energy_consumption_gwh": "sum",
                "fossil_energy_consumption_gwh": "sum",
            }
        )
        .assign(
            renewable_energy_pct=lambda x: x["renewable_energy_consumption_gwh"]
            / x["energy_consumption_gwh"],
            fossil_energy_pct=lambda x: x["fossil_energy_consumption_gwh"]
            / x["energy_consumption_gwh"],
        )
        .rename(columns={"region_name": "entity", "region_entity_code": "entity_code"})
    )


def _compute_per_capita_energy_consumption(
    energy_breakdowns_with_population_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the per-capita consumption per type across the provided geographic entities.
    """
    return energy_breakdowns_with_population_df.assign(
        energy_consumption_per_capita_gwh=lambda x: x["energy_consumption_gwh"]
        / x["population"],
        renewable_energy_per_capita_gwh=lambda x: x["renewable_energy_consumption_gwh"]
        / x["population"],
        fossil_energy_per_capita_gwh=lambda x: x["fossil_energy_consumption_gwh"]
        / x["population"],
    )


# Load the data
population_df = _get_df_from_csv(csv_population)
renewable_energy_df = _get_df_from_csv(csv_renewable_share)
energy_consumption = _get_df_from_csv(csv_energy_consumption)
regional_grouping = _get_df_from_csv(csv_regional_grouping)

# Prepare and standardize the data
# Set the common indices
population_df = _prepare_population(population_df)
renewable_energy_df = _prepare_renewable_energy(renewable_energy_df)
energy_consumption_df = _prepare_energy_consumption(energy_consumption)

# Compute the energy breakdowns
energy_breakdown = _define_fossil_and_renewable_coverage(
    energy_consumption_df, renewable_energy_df
)


# Add population
energy_breakdown_with_population = _add_population_data_to_energy_breakdown(
    energy_breakdown, population_df
)


# Create regional aggregates
energy_breakdown_with_regional_aggregates = _create_regional_rollups(
    energy_breakdown_with_population, regional_grouping
)

all_entity_data = pd.concat(
    [energy_breakdown_with_regional_aggregates, energy_breakdown_with_population]
)

# Compute per-capita metric
per_capita_metrics = _compute_per_capita_energy_consumption(all_entity_data)
