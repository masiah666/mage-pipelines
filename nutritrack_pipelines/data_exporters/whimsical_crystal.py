import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    output_path = '/home/src/vessel_arrivals_by_month.csv'
    data.to_csv(output_path, index=False)
    print(f'Wrote {len(data)} rows to {output_path}')