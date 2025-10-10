import pandas as pd

base_dir = "/workshop-repo"

# Available data files
csv_population = "data/population-with-un-projections.csv"
csv_renewable_share = "data/renewable-share-energy.csv"
csv_energy_consumption = "data/primary-energy-cons.csv"
csv_regional_grouping = "data/regional-grouping.csv"


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
