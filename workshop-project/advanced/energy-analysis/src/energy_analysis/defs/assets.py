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

from openpyxl.chart import LineChart, Reference

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


@dg.asset_check(asset=energy_breakdown_per_capita, blocking=False)
def per_capita_plausibility_check(energy_breakdown_per_capita):
    """Validate realistic per-capita energy consumption bounds"""
    max_threshold = 0.1  # 100 MWh per person per year

    violations = (energy_breakdown_per_capita["energy_consumption_per_capita"] > max_threshold).sum()
    is_valid = int(violations) == 0

    return dg.AssetCheckResult(
        passed=is_valid,
        description=f"Per-capita bounds: {violations} violations (max allowed: {max_threshold} GWh)",
    )

@dg.asset_check(asset=energy_breakdown_with_new_regions, blocking=True)
def energy_conservation_check(energy_breakdown_with_new_regions):
    """Ensure fossil + renewable percentages sum to 1.0"""
    tolerance = 1e-6

    df_with_totals = energy_breakdown_with_new_regions.assign(
        total_pct=lambda x: x["fossil_energy_pct"] + x["renewable_energy_pct"]
    )

    invalid_count = int((abs(df_with_totals["total_pct"] - 1.0) > tolerance).sum())
    is_valid = invalid_count == 0
    total = len(energy_breakdown_with_new_regions)
    return dg.AssetCheckResult(
        passed=is_valid,
        description=f"Energy conservation: {invalid_count} invalid rows out of {total}",
    )

@dg.asset()
def export_analysis_report(energy_breakdown_per_capita):
    """Export analysis report to Excel with multiple tabs and charts"""

    filename = "energy_analysis_report.xlsx"
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        latest_year_with_population = energy_breakdown_per_capita.loc[
            energy_breakdown_per_capita["population"].notna(), "year"
        ].max()

        # Remove invalid data
        valid_energy_data = energy_breakdown_per_capita.dropna(
            subset=["renewable_energy_per_capita", "fossil_energy_per_capita", "energy_consumption_per_capita"]
        ).replace([float("inf"), -float("inf")], pd.NA).dropna().reset_index()

        # Tab 1: Top 10 Renewable Energy Consumers by Max Year
        (
            valid_energy_data.loc[
                valid_energy_data.groupby("entity")[
                    "renewable_energy_per_capita"
                ].idxmax()
            ]
            .nlargest(10, "renewable_energy_per_capita")[
                ["entity", "year", "renewable_energy_per_capita"]
            ]
            .to_excel(writer, sheet_name="Top_10_Renewable_Max", index=False)
        )

        # Tab 2: Top 10 Fossil Energy Consumers by Max Year
        (
            valid_energy_data.loc[
                valid_energy_data.groupby("entity")["fossil_energy_per_capita"].idxmax()
            ]
            .nlargest(10, "fossil_energy_per_capita")[
                ["entity", "year", "fossil_energy_per_capita"]
            ]
            .to_excel(writer, sheet_name="Top_10_Fossil_Max", index=False)
        )
        # Tab 3: NAM vs IBERIA Comparison (past 10 years, pivoted by year)
        past_10_years = latest_year_with_population - 9
        nam_iberia_data = valid_energy_data.loc[
            lambda x: (x["year"] >= past_10_years)
            & (x["entity"].isin(["North America", "Iberia"]))
        ][
            [
                "entity",
                "year",
                "renewable_energy_per_capita",
                "fossil_energy_pct",
                "energy_consumption_per_capita",
            ]
        ]

        metric_labels = {
            "renewable_energy_per_capita": "Renewable_Energy_Per_Capita",
            "fossil_energy_pct": "Fossil_Energy_Percent",
            "energy_consumption_per_capita": "Energy_per_Capita",
        }

        # Pivot data with years as rows and regions as columns
        for metric in [
            "renewable_energy_per_capita",
            "fossil_energy_pct",
            "energy_consumption_per_capita",
        ]:
            pivoted = nam_iberia_data.pivot(
                index="year", columns="entity", values=metric
            )
            pivoted.to_excel(writer, sheet_name=metric_labels[metric])

        # Add chart to Renewable Energy sheet as example
        ws_renewable = writer.sheets["Renewable_Energy_Per_Capita"]
        chart = LineChart()
        chart.title = "NAM vs Iberia: Renewable Energy Per Capita Over Time"

        # Get the data range (assuming years are in column A, regions in columns B and C)
        max_row = (
            len(
                nam_iberia_data.pivot(
                    index="year",
                    columns="entity",
                    values="renewable_energy_per_capita",
                )
            )
            + 1
        )

        # Add data for North America (column B)
        data_nam = Reference(ws_renewable, min_col=2, min_row=1, max_row=max_row)
        chart.add_data(data_nam, titles_from_data=True)

        # Add data for Iberia (column C)
        data_iberia = Reference(ws_renewable, min_col=3, min_row=1, max_row=max_row)
        chart.add_data(data_iberia, titles_from_data=True)

        # Set categories (years from column A)
        categories = Reference(ws_renewable, min_col=1, min_row=2, max_row=max_row)
        chart.set_categories(categories)

        # Enable legend below the chart
        chart.legend.position = "b"  # Position legend at bottom
        chart.legend.overlay = False  # Don't overlay on chart area

        # Add chart to worksheet
        ws_renewable.add_chart(chart, "E2")

        print(f"Excel report exported to: {filename}")

        # Tab 4: Full raw data
        energy_breakdown_per_capita.to_excel(writer, sheet_name="Raw_Data", index=False)