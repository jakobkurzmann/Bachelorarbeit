from sklearn.cluster import KMeans
def kMeans(embedding, n_clusters, random_state, df_embedding):
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state).fit(embedding)
    df_embedding['cluster'] = kmeans.labels_
    print(df_embedding["cluster"].unique())
