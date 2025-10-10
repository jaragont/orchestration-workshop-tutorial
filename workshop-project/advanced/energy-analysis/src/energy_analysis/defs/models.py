import pandera as pa


class PopulationDataModel(pa.DataFrameModel):
    entity: str = pa.Field(description="Entity name")
    entity_code: str = pa.Field(description="Country code")
    year: int = pa.Field(description="Year of the population estimate")
    population: int = pa.Field(description="Population estimate")


class EnergyConsumptionDataModel(pa.DataFrameModel):
    entity: str = pa.Field(description="Entity name")
    entity_code: str = pa.Field(description="Country code")
    year: int = pa.Field(description="Year of the consumption")
    energy_consumption: float = pa.Field(description="Energy consumption in TWh")


class RenewableCoverageDataModel(pa.DataFrameModel):
    entity: str = pa.Field(description="Entity name")
    entity_code: str = pa.Field(description="Country code", nullable=True)
    year: int = pa.Field(description="Year of the estimate")
    renewable_energy_pct: float = pa.Field(description="Renewable energy coverage in %")


class RegionalGroupingDataModel(pa.DataFrameModel):
    region_entity_code: str = pa.Field(description="Region entity code")
    region_name: str = pa.Field(description="Region name")
    entity_code: str = pa.Field(description="Country code")


class EnergyBreakdownDataModel(EnergyConsumptionDataModel):
    renewable_energy_pct: float = pa.Field(description="Renewable energy coverage in %")
    fossil_energy_pct: float = pa.Field(description="Fossil energy coverage in %")
    renewable_energy_consumption: float = pa.Field(
        description="Renewable energy consumption in GWh"
    )
    fossil_energy_consumption: float = pa.Field(
        description="Fossil energy consumption in GWh"
    )


class EnergyBreakdownWithPopulationDataModel(EnergyBreakdownDataModel):
    population: int = pa.Field(description="Population", nullable=True)


class EnergyBreakdownPerCapitaDataModel(EnergyBreakdownDataModel):
    population: int = pa.Field(description="Population", nullable=True)
    energy_consumption_per_capita: float = pa.Field(
        description="Energy consumption per capita in GWh", nullable=True
    )
    renewable_energy_per_capita: float = pa.Field(
        description="Renewable energy consumption per capita in GWh", nullable=True
    )
    fossil_energy_per_capita: float = pa.Field(
        description="Fossil energy consumption per capita in GWh", nullable=True
    )
