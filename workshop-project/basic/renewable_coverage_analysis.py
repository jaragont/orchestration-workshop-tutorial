import pandas as pd

# Load all datasets
population_df = pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/population-with-un-projections.csv")
energy_df = pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/primary-energy-cons.csv")
renewable_df = pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/renewable-share-energy.csv")
taxonomy_df = pd.read_csv("/workspaces/orchestration-workshop-tutorial/data/regional-grouping.csv")

# Quick exploration
print("Dataset shapes:")
print(f"Population: {population_df.shape}")
print(f"Energy: {energy_df.shape}")
print(f"Renewable: {renewable_df.shape}")
print(f"Taxonomy: {taxonomy_df.shape}")

# Check date ranges
print("\nYear ranges:")
print(f"Population: {population_df['Year'].min()} - {population_df['Year'].max()}")
print(f"Energy: {energy_df['Year'].min()} - {energy_df['Year'].max()}")
print(f"Renewable: {renewable_df['Year'].min()} - {renewable_df['Year'].max()}")

# Check country coverage
print("\nCountry coverage:")
print(f"Countries in taxonomy: {len(taxonomy_df)}")
print(
    f"Iberia countries: {taxonomy_df[taxonomy_df['region_name'] == 'Iberia']['entity_code'].tolist()}"
)
print(
    f"North America countries: {taxonomy_df[taxonomy_df['region_name'] == 'North America']['entity_code'].tolist()}"
)
