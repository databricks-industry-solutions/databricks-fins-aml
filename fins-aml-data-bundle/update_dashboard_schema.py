#!/usr/bin/env python3
"""
Script to update dashboard JSON to use parameterized catalog and schema
or to replace hardcoded catalog.schema references.
"""
import json
import sys
import argparse

def update_dashboard_schema(dashboard_file, new_catalog, new_schema, output_file=None):
    """Update all catalog.schema references in dashboard JSON."""

    # Read the dashboard JSON
    with open(dashboard_file, 'r') as f:
        dashboard = json.load(f)

    # Count replacements
    replacements = 0

    # Update datasets
    if 'datasets' in dashboard:
        for dataset in dashboard['datasets']:
            if 'query' in dataset and dataset['query']:
                # Update single query string
                if isinstance(dataset['query'], str):
                    original = dataset['query']
                    dataset['query'] = dataset['query'].replace(
                        'fins_aml.data_generation',
                        f'{new_catalog}.{new_schema}'
                    )
                    if original != dataset['query']:
                        replacements += 1

            if 'queryLines' in dataset and dataset['queryLines']:
                # Update query lines array
                for i, line in enumerate(dataset['queryLines']):
                    original = line
                    dataset['queryLines'][i] = line.replace(
                        'fins_aml.data_generation',
                        f'{new_catalog}.{new_schema}'
                    )
                    if original != dataset['queryLines'][i]:
                        replacements += 1

    # Save updated dashboard
    output_path = output_file or dashboard_file
    with open(output_path, 'w') as f:
        json.dump(dashboard, f, indent=2)

    print(f"Updated {replacements} references from fins_aml.data_generation to {new_catalog}.{new_schema}")
    print(f"Dashboard saved to: {output_path}")

    return replacements

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update dashboard catalog and schema references')
    parser.add_argument('dashboard_file', help='Path to dashboard JSON file')
    parser.add_argument('--catalog', default='fins_aml', help='New catalog name')
    parser.add_argument('--schema', default='data_generation', help='New schema name')
    parser.add_argument('--output', help='Output file path (default: overwrite input file)')

    args = parser.parse_args()

    update_dashboard_schema(
        args.dashboard_file,
        args.catalog,
        args.schema,
        args.output
    )