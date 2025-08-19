#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sys
import argparse
from pathlib import Path

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_data(csv_file):
    """Load and clean the fairness test results"""
    try:
        df = pd.read_csv(csv_file)
        
        # Filter out ERROR and TIMEOUT results
        df = df[~df['Jains_Index'].isin(['ERROR', 'TIMEOUT'])]
        
        # Convert numeric columns
        numeric_cols = ['Insert_Ratio', 'Find_Ratio', 'Update_Ratio', 'Jains_Index', 
                       'CoV', 'Gini_Coeff', 'Throughput_Spread', 'Min_Ops', 'Max_Ops', 'Avg_Ops']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove rows with NaN values in key columns
        df = df.dropna(subset=['Lock_Type', 'Jains_Index'])
        
        print(f"Loaded {len(df)} valid test results")
        print(f"Lock types: {df['Lock_Type'].unique()}")
        
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

def create_fairness_comparison_plot(df, output_dir):
    """Create box plot comparing fairness across lock types"""
    plt.figure(figsize=(12, 8))
    
    # Box plot of Jain's Fairness Index by lock type
    sns.boxplot(data=df, x='Lock_Type', y='Jains_Index')
    plt.title('Lock Type Fairness Comparison\n(Jain\'s Fairness Index)', fontsize=16, fontweight='bold')
    plt.xlabel('Lock Type', fontsize=14)
    plt.ylabel('Jain\'s Fairness Index', fontsize=14)
    plt.ylim(0, 1)
    
    # Add horizontal lines for fairness thresholds
    plt.axhline(y=0.95, color='green', linestyle='--', alpha=0.7, label='Excellent (≥0.95)')
    plt.axhline(y=0.80, color='orange', linestyle='--', alpha=0.7, label='Good (≥0.80)')
    plt.axhline(y=0.60, color='red', linestyle='--', alpha=0.7, label='Moderate (≥0.60)')
    plt.axhline(y=0.40, color='darkred', linestyle='--', alpha=0.7, label='Poor (≥0.40)')
    
    plt.legend(loc='lower right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fairness_comparison_boxplot.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_operation_ratio_heatmap(df, output_dir):
    """Create heatmaps showing fairness vs operation ratios for each lock type"""
    lock_types = df['Lock_Type'].unique()
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Fairness Heatmaps by Lock Type and Operation Ratios', fontsize=16, fontweight='bold')
    
    axes = axes.flatten()
    
    for i, lock_type in enumerate(lock_types):
        if i >= len(axes):
            break
            
        lock_data = df[df['Lock_Type'] == lock_type]
        
        # Create pivot table for heatmap
        pivot_data = lock_data.pivot_table(
            values='Jains_Index', 
            index='Insert_Ratio', 
            columns='Find_Ratio', 
            aggfunc='mean'
        )
        
        # Create heatmap
        sns.heatmap(pivot_data, annot=True, fmt='.3f', cmap='RdYlGn', 
                   vmin=0, vmax=1, ax=axes[i], cbar_kws={'label': 'Jain\'s Index'})
        axes[i].set_title(f'{lock_type}', fontweight='bold')
        axes[i].set_xlabel('Find Ratio')
        axes[i].set_ylabel('Insert Ratio')
    
    # Hide unused subplots
    for j in range(len(lock_types), len(axes)):
        axes[j].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fairness_heatmaps.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_violin_plot(df, output_dir):
    """Create violin plot showing distribution of fairness scores"""
    plt.figure(figsize=(12, 8))
    
    sns.violinplot(data=df, x='Lock_Type', y='Jains_Index', inner='quartile')
    plt.title('Distribution of Fairness Scores by Lock Type', fontsize=16, fontweight='bold')
    plt.xlabel('Lock Type', fontsize=14)
    plt.ylabel('Jain\'s Fairness Index', fontsize=14)
    plt.ylim(0, 1)
    
    # Add mean values as points
    means = df.groupby('Lock_Type')['Jains_Index'].mean()
    for i, (lock_type, mean_val) in enumerate(means.items()):
        plt.scatter(i, mean_val, color='red', s=100, zorder=10, marker='D', label='Mean' if i == 0 else '')
    
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/fairness_violin_plot.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_scatter_matrix(df, output_dir):
    """Create scatter plots showing relationships between metrics"""
    metrics = ['Jains_Index', 'CoV', 'Gini_Coeff', 'Throughput_Spread']
    
    # Filter to only include columns that exist and have numeric data
    available_metrics = [m for m in metrics if m in df.columns and df[m].dtype in ['float64', 'int64']]
    
    if len(available_metrics) < 2:
        print("Not enough numeric metrics for scatter matrix")
        return
    
    fig, axes = plt.subplots(len(available_metrics), len(available_metrics), figsize=(16, 16))
    fig.suptitle('Fairness Metrics Correlation Matrix', fontsize=16, fontweight='bold')
    
    colors = {'MUTEX': 'red', 'SPINLOCK': 'green', 'RWLOCK': 'blue', 'ADAPTIVE_MUTEX': 'orange'}
    
    for i, metric_y in enumerate(available_metrics):
        for j, metric_x in enumerate(available_metrics):
            ax = axes[i, j]
            
            if i == j:
                # Diagonal: histograms
                for lock_type in df['Lock_Type'].unique():
                    lock_data = df[df['Lock_Type'] == lock_type]
                    ax.hist(lock_data[metric_x], alpha=0.5, label=lock_type, 
                           color=colors.get(lock_type, 'gray'), bins=15)
                ax.set_xlabel(metric_x)
                if j == 0:
                    ax.legend()
            else:
                # Off-diagonal: scatter plots
                for lock_type in df['Lock_Type'].unique():
                    lock_data = df[df['Lock_Type'] == lock_type]
                    ax.scatter(lock_data[metric_x], lock_data[metric_y], 
                             alpha=0.6, label=lock_type if i == 0 else '', 
                             color=colors.get(lock_type, 'gray'), s=50)
                
                ax.set_xlabel(metric_x)
                ax.set_ylabel(metric_y)
                
                if i == 0 and j == len(available_metrics) - 1:
                    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/metrics_scatter_matrix.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_operation_ratio_impact(df, output_dir):
    """Create plots showing how operation ratios impact fairness"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Impact of Operation Ratios on Fairness', fontsize=16, fontweight='bold')
    
    # Plot 1: Insert Ratio vs Fairness
    for lock_type in df['Lock_Type'].unique():
        lock_data = df[df['Lock_Type'] == lock_type]
        axes[0, 0].scatter(lock_data['Insert_Ratio'], lock_data['Jains_Index'], 
                          label=lock_type, alpha=0.7, s=50)
    axes[0, 0].set_xlabel('Insert Ratio')
    axes[0, 0].set_ylabel('Jain\'s Fairness Index')
    axes[0, 0].set_title('Insert Ratio Impact')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Plot 2: Find Ratio vs Fairness
    for lock_type in df['Lock_Type'].unique():
        lock_data = df[df['Lock_Type'] == lock_type]
        axes[0, 1].scatter(lock_data['Find_Ratio'], lock_data['Jains_Index'], 
                          label=lock_type, alpha=0.7, s=50)
    axes[0, 1].set_xlabel('Find Ratio')
    axes[0, 1].set_ylabel('Jain\'s Fairness Index')
    axes[0, 1].set_title('Find Ratio Impact')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    
    # Plot 3: Update Ratio vs Fairness
    for lock_type in df['Lock_Type'].unique():
        lock_data = df[df['Lock_Type'] == lock_type]
        axes[1, 0].scatter(lock_data['Update_Ratio'], lock_data['Jains_Index'], 
                          label=lock_type, alpha=0.7, s=50)
    axes[1, 0].set_xlabel('Update Ratio')
    axes[1, 0].set_ylabel('Jain\'s Fairness Index')
    axes[1, 0].set_title('Update Ratio Impact')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # Plot 4: Combined ratios (Insert + Update vs Find)
    df['Combined_Write_Ratio'] = df['Insert_Ratio'] + df['Update_Ratio']
    for lock_type in df['Lock_Type'].unique():
        lock_data = df[df['Lock_Type'] == lock_type]
        axes[1, 1].scatter(lock_data['Find_Ratio'], lock_data['Combined_Write_Ratio'], 
                          c=lock_data['Jains_Index'], label=lock_type, alpha=0.7, s=50, 
                          cmap='RdYlGn', vmin=0, vmax=1)
    axes[1, 1].set_xlabel('Find Ratio (Read Operations)')
    axes[1, 1].set_ylabel('Write Ratio (Insert + Update)')
    axes[1, 1].set_title('Read vs Write Balance (colored by fairness)')
    cbar = plt.colorbar(axes[1, 1].collections[0], ax=axes[1, 1])
    cbar.set_label('Jain\'s Fairness Index')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/operation_ratio_impact.png', dpi=300, bbox_inches='tight')
    plt.close()

def create_summary_statistics(df, output_dir):
    """Create a summary statistics table and save as image"""
    # Calculate summary statistics
    summary_stats = df.groupby('Lock_Type').agg({
        'Jains_Index': ['mean', 'std', 'min', 'max', 'count'],
        'CoV': ['mean'],
        'Gini_Coeff': ['mean'],
        'Throughput_Spread': ['mean']
    }).round(4)
    
    # Flatten column names
    summary_stats.columns = ['_'.join(col).strip() for col in summary_stats.columns]
    
    # Create a figure for the table
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('tight')
    ax.axis('off')
    
    # Create table
    table_data = summary_stats.reset_index()
    table = ax.table(cellText=table_data.values, colLabels=table_data.columns, 
                    cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
    
    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Color header
    for i in range(len(table_data.columns)):
        table[(0, i)].set_facecolor('#40466e')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Color rows alternately
    for i in range(1, len(table_data) + 1):
        for j in range(len(table_data.columns)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#f1f1f2')
    
    plt.title('Summary Statistics by Lock Type', fontsize=16, fontweight='bold', pad=20)
    plt.savefig(f'{output_dir}/summary_statistics.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Also save as CSV
    summary_stats.to_csv(f'{output_dir}/summary_statistics.csv')
    
    return summary_stats

def create_performance_ranking(df, output_dir):
    """Create ranking chart of lock types"""
    # Calculate average metrics per lock type
    rankings = df.groupby('Lock_Type').agg({
        'Jains_Index': 'mean',
        'CoV': 'mean', 
        'Gini_Coeff': 'mean'
    }).round(4)
    
    # Sort by Jain's Index (higher is better)
    rankings = rankings.sort_values('Jains_Index', ascending=True)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Create horizontal bar chart
    y_pos = np.arange(len(rankings))
    bars = ax.barh(y_pos, rankings['Jains_Index'], alpha=0.7)
    
    # Color bars based on performance
    colors = []
    for val in rankings['Jains_Index']:
        if val >= 0.95:
            colors.append('green')
        elif val >= 0.80:
            colors.append('orange') 
        elif val >= 0.60:
            colors.append('yellow')
        elif val >= 0.40:
            colors.append('red')
        else:
            colors.append('darkred')
    
    for bar, color in zip(bars, colors):
        bar.set_color(color)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(rankings.index)
    ax.set_xlabel('Average Jain\'s Fairness Index')
    ax.set_title('Lock Type Performance Ranking', fontsize=16, fontweight='bold')
    ax.set_xlim(0, 1)
    
    # Add value labels on bars
    for i, (idx, row) in enumerate(rankings.iterrows()):
        ax.text(row['Jains_Index'] + 0.01, i, f'{row["Jains_Index"]:.4f}', 
                va='center', fontweight='bold')
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/performance_ranking.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Generate fairness analysis graphs from CSV results')
    parser.add_argument('csv_file', help='Path to the CSV results file')
    parser.add_argument('--output', '-o', default='./graphs', help='Output directory for graphs')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)
    
    print(f"Loading data from {args.csv_file}")
    df = load_data(args.csv_file)
    
    print(f"Generating graphs in {output_dir}")
    
    # Generate all graphs
    print("Creating fairness comparison plot...")
    create_fairness_comparison_plot(df, output_dir)
    
    print("Creating operation ratio heatmaps...")
    create_operation_ratio_heatmap(df, output_dir)
    
    print("Creating violin plot...")
    create_violin_plot(df, output_dir)
    
    print("Creating scatter matrix...")
    create_scatter_matrix(df, output_dir)
    
    print("Creating operation ratio impact plots...")
    create_operation_ratio_impact(df, output_dir)
    
    print("Creating summary statistics...")
    summary_stats = create_summary_statistics(df, output_dir)
    
    print("Creating performance ranking...")
    create_performance_ranking(df, output_dir)
    
    print(f"\nAll graphs saved to {output_dir}/")
    print("\nGenerated files:")
    print("  - fairness_comparison_boxplot.png")
    print("  - fairness_heatmaps.png") 
    print("  - fairness_violin_plot.png")
    print("  - metrics_scatter_matrix.png")
    print("  - operation_ratio_impact.png")
    print("  - summary_statistics.png")
    print("  - summary_statistics.csv")
    print("  - performance_ranking.png")
    
    print(f"\nSummary Statistics:")
    print(summary_stats)

if __name__ == "__main__":
    main()
