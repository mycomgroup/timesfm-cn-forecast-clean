import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import json
import sys

def generate_clusters():
    db_path = "data/market.duckdb"
    con = duckdb.connect(db_path, read_only=True)
    
    print("Loading price data for all symbols (last 365 days)...")
    df = con.execute("""
        SELECT symbol, date, close 
        FROM daily_data 
        WHERE date >= (SELECT MAX(date) - INTERVAL '365 days' FROM daily_data)
    """).fetchdf()
    con.close()
    
    if df.empty:
        print("Error: No data found.")
        return

    print(f"Data loaded: {len(df)} rows. Deduplicating...")
    # Handle duplicates by taking the last entry for each (date, symbol)
    df = df.sort_values('date').drop_duplicates(subset=['date', 'symbol'], keep='last')
    
    print("Pivoting data...")
    pivot_df = df.pivot(index='date', columns='symbol', values='close')
    print(f"Pivoted shape: {pivot_df.shape}")
    
    # Calculate daily returns
    returns_df = pivot_df.pct_change()
    
    # Filter for symbols with enough history (at least 200 trading days)
    valid_symbols = returns_df.columns[returns_df.count() > 200]
    returns_df = returns_df[valid_symbols]
    print(f"Valid symbols after filtering: {len(valid_symbols)}")
    
    print(f"Calculating correlation matrix...")
    # Optimization: drop rows where all are NaN before correlation
    returns_df = returns_df.dropna(how='all')
    corr = returns_df.corr()
    
    # Seeds (Resonance anchors)
    seeds = ['sh600977', 'sh603589', 'sh688095', 'sz300057', 'sh688695', 
             'sh600519', 'sz300656', 'sz300292', 'sh688152', 'sz301327']

    clusters = {}
    for seed in seeds:
        if seed not in corr.columns:
            print(f"Warning: Seed {seed} not found.")
            continue
            
        # Find peers with correlation > 0.7
        peers = corr[seed][corr[seed] > 0.7].sort_values(ascending=False)
        # Filter out self
        peers = peers.drop(labels=[seed], errors='ignore')
        
        # Limit to top 50 members
        members = peers.head(50).index.tolist()
        
        # Always include the seed itelf
        members = [seed] + members
        
        if len(members) >= 5:
            clusters[f"resonance_{seed}"] = members
            print(f"Created 'resonance_{seed}' with {len(members)} members.")

    output_file = "data/resonance_clusters.json"
    with open(output_file, "w") as f:
        json.dump(clusters, f, indent=2)
    
    print(f"\nSaved {len(clusters)} clusters to {output_file}")

if __name__ == "__main__":
    generate_clusters()
