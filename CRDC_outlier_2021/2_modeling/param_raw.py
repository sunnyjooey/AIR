# these are the defaults
# pyod.models.cblof.CBLOF(n_clusters=8, contamination=0.1, clustering_estimator=None, alpha=0.9, beta=5, use_weights=False, check_estimator=False, random_state=None, n_jobs=1)

from sklearn.cluster import KMeans

# parameter vals to try for each classifier and parameter
param_grid_all = {
 'CBLOF': {
      'n_clusters':[20],
      'contamination':[0.01],
      'clustering_estimator':[KMeans(n_init=10, max_iter=1000, tol=0.0001, algorithm='full')], 
      'alpha':[0.9], 
      'beta':[5], 
      'use_weights':[False], 
      'check_estimator':[False]
          }
}