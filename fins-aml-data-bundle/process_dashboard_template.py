#!/usr/bin/env python3
"""
Process the dashboard template file to replace catalog and schema placeholders.
This script is meant to be run during Databricks bundle deployment.
"""

import json
import sys
import os
import argparse


def process_dashboard_template(template_path, output_path, catalog, schema):
    """
    Process the dashboard template file and replace placeholders with actual values.

    Args:
        template_path: Path to the template dashboard file
        output_path: Path where the processed dashboard file will be saved
        catalog: The catalog name to use
        schema: The schema name to use
    """
    print(f"Processing dashboard template...")
    print(f"  Template: {template_path}")
    print(f"  Output: {output_path}")
    print(f"  Catalog: {catalog}")
    print(f"  Schema: {schema}")

    # Read the template file
    with open(template_path, 'r') as f:
        content = f.read()

    # Replace placeholders
    content = content.replace('${catalog}', catalog)
    content = content.replace('${schema}', schema)

    # Parse JSON to ensure it's valid
    try:
        dashboard_json = json.loads(content)
        # Pretty print the JSON for readability
        processed_content = json.dumps(dashboard_json, indent=2)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON after processing: {e}")
        sys.exit(1)

    # Write the processed content to the output file
    with open(output_path, 'w') as f:
        f.write(processed_content)

    print(f"✅ Dashboard processed successfully!")

    # Verify replacements were made
    verify_replacements(dashboard_json)


def verify_replacements(dashboard_json):
    """Verify that all placeholders have been replaced."""
    dashboard_str = json.dumps(dashboard_json)

    if '${catalog}' in dashboard_str or '${schema}' in dashboard_str:
        print("⚠️  Warning: Some placeholders may not have been replaced!")
    else:
        print("✅ All placeholders successfully replaced.")

    # Count table references
    count = 0
    for dataset in dashboard_json.get('datasets', []):
        for query_line in dataset.get('queryLines', []):
            if 'FROM' in query_line or 'from' in query_line:
                count += 1
    print(f"📊 Found {len(dashboard_json.get('datasets', []))} datasets in dashboard")


def main():
    parser = argparse.ArgumentParser(
        description='Process Databricks dashboard template with catalog/schema values'
    )
    parser.add_argument('--template', '-t',
                        default='SherlockAML_ExecDash_Template.lvdash.json',
                        help='Path to template dashboard file')
    parser.add_argument('--output', '-o',
                        default='SherlockAML_ExecDash_Final.lvdash.json',
                        help='Path to output dashboard file')
    parser.add_argument('--catalog', '-c',
                        required=True,
                        help='Catalog name to use')
    parser.add_argument('--schema', '-s',
                        required=True,
                        help='Schema name to use')

    args = parser.parse_args()

    # Check if template file exists
    if not os.path.exists(args.template):
        print(f"Error: Template file not found: {args.template}")
        sys.exit(1)

    process_dashboard_template(args.template, args.output, args.catalog, args.schema)


if __name__ == '__main__':
    main()