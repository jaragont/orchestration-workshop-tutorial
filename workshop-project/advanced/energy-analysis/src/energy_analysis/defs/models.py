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
