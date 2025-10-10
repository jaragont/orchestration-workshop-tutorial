import dagster as dg
import pandas as pd
from dagster_pandera import pandera_schema_to_dagster_type


from energy_analysis.defs.models import (
    PopulationDataModel,
    EnergyConsumptionDataModel,
    RenewableCoverageDataModel,
    RegionalGroupingDataModel,
)


@dg.asset(dagster_type=pandera_schema_to_dagster_type(PopulationDataModel.to_schema()))
def population():
    """Population by country from UN projections"""
    return (
        pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/population-with-un-projections.csv")
        .rename(
            columns={
                "population__sex_all__age_all__variant_estimates": "population",
                "Entity": "entity",
                "Code": "entity_code",
                "Year": "year",
            }
        )
        .dropna(subset=["population"])
        .astype({"year": int, "population": int, "entity_code": str, "entity": str})
    )


@dg.asset(
    dagster_type=pandera_schema_to_dagster_type(EnergyConsumptionDataModel.to_schema())
)
def energy_consumption():
    """Energy consumption by country from UN projections"""
    return (
        pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/primary-energy-cons.csv")
        .rename(
            columns={
                "primary_energy_consumption__twh": "energy_consumption",
                "Entity": "entity",
                "Code": "entity_code",
                "Year": "year",
            }
        )
        .astype(
            {
                "year": int,
                "energy_consumption": float,
                "entity_code": str,
                "entity": str,
            }
        )
    )


@dg.asset(
    dagster_type=pandera_schema_to_dagster_type(RenewableCoverageDataModel.to_schema())
)
def renewable_coverage():
    """Renewable energy coverage by country from UN projections"""
    return (
        pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/renewable-share-energy.csv")
        .rename(
            columns={
                "renewables__pct_equivalent_primary_energy": "renewable_energy_pct",
                "Entity": "entity",
                "Code": "entity_code",
                "Year": "year",
            }
        )
        .assign(renewable_energy_pct=lambda x: x["renewable_energy_pct"] / 100)
    )


@dg.asset(
    dagster_type=pandera_schema_to_dagster_type(RegionalGroupingDataModel.to_schema())
)
def regional_grouping():
    """Regional grouping taxonomy"""
    return pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/regional-grouping.csv")
