import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from dataEngining import generateDateFrame


def enforce_max_cluster_size(X, labels, centers, max_size):
    """
    Reassigns points from oversized clusters to the nearest other cluster.
    """
    new_labels = labels.copy()
    k = len(centers)

    for c in range(k):
        idx = np.where(new_labels == c)[0]

        if len(idx) > max_size:
            # Distances to centroid
            dists = np.linalg.norm(X[idx] - centers[c], axis=1)

            # Overflow = farthest points
            overflow = idx[np.argsort(dists)[max_size:]]

            # Reassign overflow points
            for i in overflow:
                d = np.linalg.norm(centers - X[i], axis=1)
                d[c] = np.inf  # prevent returning to same cluster
                new_labels[i] = np.argmin(d)

    return new_labels


def apply_weights(df, weights):
    """
    Applies weights only to columns that exist in the encoded dataframe.
    Prevents KeyErrors.
    """
    for col, w in weights.items():
        if col in df.columns:
            df[col] *= w
    return df


def runMode(files):
    # -----------------------------
    # 1. Load & preprocess dataframe
    # -----------------------------
    df, maxSize = generateDateFrame(files)

    # Select numeric columns
    numeric_df = df.select_dtypes(include=['number'])

    # One-hot encode
    encoded = pd.get_dummies(numeric_df, dummy_na=False)
    encoded = encoded.fillna(-1)
    encoded.columns = encoded.columns.astype(str)

    # -----------------------------
    # 2. Apply weights
    # -----------------------------
    weights = {
        "Crossing Start Time": 0.0,
        "Crossing End Time": 0.0,
        "Bus Stop Arrival Time": 0.0,
        "Bus Stop Departure Time": 0.0,
        'Bus Stop Arrival Time (sin)':9,
        'Bus Stop Arrival Time (cos)':9,
        'Bus Stop Departure Time (sin)':9, 
        'Bus Stop Departure Time (cos)':9,
        'Intend to Cross Timestamp (sin)':9,
        'Intend to Cross Timestamp (cos)':9,
        'Crossing Start Time (sin)':9, 
        'Crossing Start Time (cos)':9,
        'Refuge Island Start Time (sin)':9,
        'Refuge Island Start Time (cos)':9,
        'Refuge Island End Time (sin)':9,
        'Refuge Island End Time (cos)':9,
        'Crossing End Time (sin)':9,
        'Crossing End Time (cos)'
        "User Type": 6,
        "Vehicle Traffic": 2,
        "Clothing Color": 8,
        "Roadway Crossing": 5,
        "Group Size": 4,
        "Bus Interaction": 3,
        "Estimated Visible Distrction": 2,
        "Estimated Gender": 8,
        "Bus Presence": 4,
        "Intend to Cross Timestamp": 3,
        "Crosswalk Crossing?": 6,
        "Crossing Location Relative to Bus Stop": 8,
        "Pedestrian Phase Crossing?": 8,
        "Did User Finish Crossing During Pedestrian Phase?": 6,
        "Type of Bus Interaction": 4,
        "Refuge Island": 0.0,
        "Refuge Island End Time": 0.0,
        "Refuge Island Start Time": 0.0,
        "Crossing Location Relative to Bus": 0.0,
        "General Reviewer Notes": 0.0
    }

    encoded = apply_weights(encoded, weights)

    # -----------------------------
    # 3. Scale + cluster
    # -----------------------------
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(encoded)

    kmeans = KMeans(n_clusters=maxSize, random_state=None)
    labels = kmeans.fit_predict(X_scaled)

    labels = enforce_max_cluster_size(
        X_scaled,
        labels,
        kmeans.cluster_centers_,
        max_size=20
    )

    df["cluster"] = labels

    # -----------------------------
    # 4. Export clusters
    # -----------------------------
    for cid, group in df.groupby("cluster"):
        filename = f"./computeData/cluster_{cid}.csv"
        group.to_csv(filename, index=False)
        print(f"Saved {filename} with {len(group)} rows")

    # -----------------------------
    # 5. PCA + loadings export
    # -----------------------------
    pca = PCA(n_components=2)
    X_2d = pca.fit_transform(X_scaled)

    loadings = pd.DataFrame(
        pca.components_.T,
        columns=['PC1_weight', 'PC2_weight'],
        index=encoded.columns
    )

    loadings["PC1_abs"] = loadings["PC1_weight"].abs()
    loadings["PC2_abs"] = loadings["PC2_weight"].abs()

    loadings.sort_values("PC1_abs", ascending=False).to_csv(
        "./computeData/pca_loadings.csv"
    )


# -----------------------------
# Run
# -----------------------------
files = [
    "./resource/Belmont+Edward_St/Neva.csv",
    "./resource/Belmont+Edward_St/Primah.csv",
    "./resource/Belmont+Edward_St/Gareth.csv"
]

runMode(files)
