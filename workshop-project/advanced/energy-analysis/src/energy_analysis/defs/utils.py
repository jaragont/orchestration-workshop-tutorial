from dagster_pandera import pandera_schema_to_dagster_type
import pandera as pa


def get_dagster_type(model: pa.DataFrameModel, asset_name: str | None = None) -> None:
    dagster_type = None
    schema = model.to_schema()
    if asset_name:
        schema.title = f"{model.Config.name}_{asset_name}"
    else:
        schema.title = model.Config.name
    dagster_type = pandera_schema_to_dagster_type(schema)
    return dagster_type
