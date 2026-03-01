import os
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

from config import EXCLUDED_FROM_ACCURACY, OUTPUT_PATH
from traffic_research.processing.quality_control import parseEnumObjectRow


# Time columns to exclude from clustering features
ExcludedColumns = [
    'Bus Stop Arrival Time', 'Bus Stop Departure Time', 'Intend to Cross Timestamp',
    'Crossing Start Time', 'Refuge Island Start Time', 'Refuge Island End Time',
    'Crossing End Time','Bus Stop IDs/Addresses','Crosswalk Location Relative to Bus Stop',
    'User Count','Crossing Treatment','Video Title','Initials','Location Name','User Notes',
    'Noteworthy Events',
    'General Reviewer Notes',
    'Bus Noteworthy Events','Count of Bus Stop Routes',
]


def featureSelection(df):
    """Return dataframe with only columns not in EXCLUDED_FROM_ACCURACY and not time fields."""
    excluded_from_accuracy = list(ExcludedColumns)

    cols = [c for c in df.columns if c not in excluded_from_accuracy]
    return df[cols]

def parseGroup(group):
    groupRows = []
    for index, row in group.iterrows():
        groupRows.append(parseEnumObjectRow(row))
    return pd.DataFrame(groupRows)


def visualize_clusters(X_scaled, labels, n_clusters, output_path):
    """Reduce scaled features to 2D with PCA and scatter-plot by cluster."""
    pca = PCA(n_components=2, random_state=None)
    X_2d = pca.fit_transform(X_scaled)

    fig, ax = plt.subplots(figsize=(10, 8))
    scatter = ax.scatter(
        X_2d[:, 0], X_2d[:, 1],
        c=labels, cmap="tab20", alpha=0.7, edgecolors="white", linewidths=0.3
    )
    ax.set_xlabel(f"PC1 ({100 * pca.explained_variance_ratio_[0]:.1f}% variance)")
    ax.set_ylabel(f"PC2 ({100 * pca.explained_variance_ratio_[1]:.1f}% variance)")
    ax.set_title("Cluster visualization (PCA of scaled features)")
    cbar = plt.colorbar(scatter, ax=ax, ticks=range(n_clusters), label="Cluster")
    plt.tight_layout()
    plt.savefig(output_path+'/cluster_visualization.png', dpi=150)
    plt.close()
    print(f"Saved cluster visualization to {output_path}")


def runMode(df, n_clusters):
    # -----------------------------
    # 1. Load & preprocess dataframe
    # -----------------------------
    # Use only columns not in EXCLUDED_FROM_ACCURACY for clustering
    df_for_clustering = featureSelection(df)
    # Select numeric columns
    numeric_df = df_for_clustering.select_dtypes(include=['number'])

    # One-hot encode
    encoded = pd.get_dummies(numeric_df, dummy_na=False)
    encoded = encoded.fillna(-1)
    encoded.columns = encoded.columns.astype(str)


    # -----------------------------
    # 3. Scale + cluster
    # -----------------------------
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(encoded)

    kmeans = KMeans(n_clusters=n_clusters, random_state=None)
    labels = kmeans.fit_predict(X_scaled)

    # Visualize clusters in 2D (PCA)
    outputClusterFolderPath = os.path.join(OUTPUT_PATH, 'cluster')
    visualize_clusters(X_scaled, labels, n_clusters,outputClusterFolderPath)

    # Assign cluster labels to full dataframe (keep all columns in output)
    df["cluster"] = labels
    for cid, group in df.groupby("cluster"):
        groupDf = parseGroup(group)
        filename = f"cluster_{cid}.csv"
        groupDf.to_csv(os.path.join(outputClusterFolderPath, filename), index=False)
