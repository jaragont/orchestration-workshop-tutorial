import dagster as dg
import pandas as pd
from dagster_pandera import pandera_schema_to_dagster_type
from energy_analysis.defs.utils import get_dagster_type

from energy_analysis.defs.models import (
    PopulationDataModel,
    EnergyConsumptionDataModel,
    RenewableCoverageDataModel,
    RegionalGroupingDataModel,
    EnergyBreakdownDataModel,
    EnergyBreakdownWithPopulationDataModel,
    EnergyBreakdownPerCapitaDataModel,
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


@dg.asset(
    dagster_type=pandera_schema_to_dagster_type(EnergyBreakdownDataModel.to_schema())
)
def energy_breakdown(energy_consumption, renewable_coverage):
    """Combine energy consumption with renewable percentages to calculate fossil vs renewable breakdown"""
    return energy_consumption.merge(
        renewable_coverage, how="left", on=["entity", "entity_code", "year"]
    ).assign(
        renewable_energy_pct=lambda x: x["renewable_energy_pct"].fillna(0),
        fossil_energy_pct=lambda x: 1 - x["renewable_energy_pct"],
        renewable_energy_consumption=lambda x: x["energy_consumption"]
        * x["renewable_energy_pct"],
        fossil_energy_consumption=lambda x: x["energy_consumption"]
        * x["fossil_energy_pct"],
    )


@dg.asset(
    dagster_type=pandera_schema_to_dagster_type(
        EnergyBreakdownWithPopulationDataModel.to_schema()
    )
)
def energy_breakdown_with_population(energy_breakdown, population):
    """Combine energy breakdown with population data"""
    return energy_breakdown.merge(
        population, how="left", on=["entity", "entity_code", "year"]
    ).astype({"population": "Int64"})


@dg.asset(
    dagster_type=get_dagster_type(
        EnergyBreakdownWithPopulationDataModel, "energy_breakdown_with_new_regions"
    )
)
def energy_breakdown_with_new_regions(
    energy_breakdown_with_population, regional_grouping
):
    """Combine energy breakdown with new regional data"""
    entities_of_interest = energy_breakdown_with_population.merge(
        regional_grouping, on="entity_code"
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
                "energy_consumption": "sum",
                "renewable_energy_consumption": "sum",
                "fossil_energy_consumption": "sum",
            }
        )
        .assign(
            renewable_energy_pct=lambda x: x["renewable_energy_consumption"]
            / x["energy_consumption"],
            fossil_energy_pct=lambda x: x["fossil_energy_consumption"]
            / x["energy_consumption"],
        )
        .rename(columns={"region_name": "entity", "region_entity_code": "entity_code"})
    )


@dg.asset(dagster_type=get_dagster_type(EnergyBreakdownPerCapitaDataModel))
def energy_breakdown_per_capita(
    energy_breakdown_with_population, energy_breakdown_with_new_regions
):
    """Compute per-capita energy consumption metrics"""
    all_breakdowns = pd.concat(
        [energy_breakdown_with_population, energy_breakdown_with_new_regions]
    )
    return all_breakdowns.assign(
        energy_consumption_per_capita=lambda x: x["energy_consumption"]
        / x["population"],
        renewable_energy_per_capita=lambda x: x["renewable_energy_consumption"]
        / x["population"],
        fossil_energy_per_capita=lambda x: x["fossil_energy_consumption"]
        / x["population"],
    )
