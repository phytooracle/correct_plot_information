#!/usr/bin/env python3
"""
Author : Emmanuel Gonzalez
Date   : 2023-11-07
Purpose: Fix shapefile error
"""

import argparse
import os
import sys
import geopandas as gpd
import pandas as pd
import glob
import seaborn as sns
from tqdm import tqdm

# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description='Rock the Casbah',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('data_path',
                        metavar='str',
                        help='Path to data that needs to be fixed.')

    parser.add_argument('-g',
                        '--geojson_path',
                        help='Path to GeoJSON file.',
                        metavar='str',
                        type=str,
                        required=True)

    parser.add_argument('-c',
                        '--columns_to_replace',
                        help='Columns to replace',
                        metavar='str',
                        type=str,
                        nargs='+',
                        default=['plot', 'year', 'range', 'row', 'species', 'treatment', 'type', 'rep', 'accession'])
    
    parser.add_argument('-f',
                        '--fieldbook_information',
                        help='Output the full fieldbook information.',
                        action='store_true',
                        default=False)

    return parser.parse_args()


# --------------------------------------------------
def get_error_plots_geodataframe(geojson_path):
    
    # Read GeoJSON file
    gdf = gpd.read_file(geojson_path)

    # Extract relevant columns
    gdf = gdf[['year', 'range', 'row', 'plot', 'type', 'rep', 'treatment', 'species', 'accession', 'Entry_ID', 'ID', 'error_ID', 'mismatch']]

    # Extract only those plots that had error
    gdf = gdf[gdf['mismatch']=='error']

    return gdf


# --------------------------------------------------
def get_error_plots_list(gdf):

    # Get a list of those plots that had error
    error_plot_list = gdf['error_ID'].unique().tolist()

    return error_plot_list


# --------------------------------------------------
def fix_error_plots(geojson_path, data_path, output_directory, error_plot_list, gdf, columns_to_replace):
    # Get arguments
    args = get_args()

    # Get list of all CSV files
    csv_files = [os.path.join(root, file) for root, dirs, files in os.walk(data_path) for file in files if file.endswith(".csv")]

    # Iterate through all CSV files with a progress bar
    for csv in tqdm(csv_files, desc="Processing CSV files", unit="file"):

        # Read CSV
        temp_df = pd.read_csv(csv)

        # Make all column names lowercase
        temp_df.columns = temp_df.columns.str.lower()

        # Convert plot column to string
        temp_df['plot'] = temp_df['plot'].astype(str)

        # Initialize the "replaced" column with False
        temp_df['replaced'] = False
                
        # Find the rows where 'plot' is in error_plot_list
        mask = temp_df['plot'].isin(error_plot_list)
                
        # Get the corresponding rows from gdf
        temp_gdf = gdf.set_index('error_ID').loc[temp_df.loc[mask, 'plot']]
                
        # Fill in the NA values with the values from temp_gdf
        for col in columns_to_replace:
            temp_df.loc[mask, col] = temp_gdf[col].values
                
        # Indicate that values have been replaced
        temp_df.loc[mask, 'replaced'] = True
                
        # Sort values by plot
        temp_df = temp_df.sort_values('plot')

        # Drop unnecessary column
        temp_df = temp_df.drop('unnamed: 0', axis=1, errors='ignore')

        # Get a list of original column names
        cols = temp_df.columns.tolist()

        # Get list of shared columns between temp_df and gdf
        common_columns = temp_df.columns.intersection(gdf.columns).tolist()
        
        # Drop shared columns in temp_df to prevent errors
        if 'plot' in common_columns:
            common_columns.remove('plot')

        if common_columns:
            temp_df = temp_df.drop(common_columns, axis=1, errors='ignore')
        
        # Merge temp_df and gdf
        temp_df = gdf.merge(temp_df, on='plot')
        
        if args.fieldbook_information:
            items = ['plot', 'year', 'range', 'row', 'species', 'treatment', 'type', 'rep', 'accession']

            for item in reversed(items):  # reverse the list to maintain the order when inserting at the front
                if item not in cols:
                    cols.insert(0, item)

        # Keep only columns in the original dataframe and their order
        temp_df = temp_df[cols]
        
        # Save to file
        csv_outname = os.path.basename(csv)
        temp_df.to_csv(os.path.join(output_directory, csv_outname.replace('.csv', '_corrected.csv')), index=False)



# --------------------------------------------------
def main():
    """Correct phenotype information here."""

    # Get arguments
    args = get_args()

    # Define and create output directory
    out_path = ''.join([args.data_path.rstrip(os.sep), '_corrected'])

    if not os.path.isdir(out_path):
        os.makedirs(out_path)

    # Get error plots to fix
    gdf = get_error_plots_geodataframe(geojson_path=args.geojson_path)

    # Get list of error plots to fix
    error_plot_list = get_error_plots_list(gdf=gdf)

    print(f'Fixing plots.')

    # Fix error plots using GeoJSON
    fix_error_plots(
        geojson_path=args.geojson_path,
        data_path=args.data_path,
        output_directory=out_path,
        error_plot_list=error_plot_list, 
        gdf=gdf,
        columns_to_replace=args.columns_to_replace
    )

    print(f'Finished fixing plot information, see {out_path}.')

# --------------------------------------------------
if __name__ == '__main__':
    main()
