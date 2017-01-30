# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import pandas as pd
import time

print "importing data"
wall,clock = time.time(),time.clock()
user_ids = []
subreddit_ids = []
subreddit_to_id = {}
i=0
with open("reddit_user_posting_behavior.csv", 'r') as f:
    for line in f:
        for sr in line.rstrip().split(",")[1:]: 
            if sr not in subreddit_to_id: 
                subreddit_to_id[sr] = len(subreddit_to_id)
            user_ids.append(i)
            subreddit_ids.append(subreddit_to_id[sr])
        i+=1

print "done importing data in ", time.time() - wall, " and ", time.clock() - clock;

print "importing in to sparse matrix"
wall,clock = time.time(),time.clock()

import numpy as np
from scipy.sparse import csr_matrix 

rows = np.array(subreddit_ids)
cols = np.array(user_ids)
data = np.ones((len(user_ids),))
num_rows = len(subreddit_to_id)
num_cols = i

# the code above exists to feed this call
adj = csr_matrix( (data,(rows,cols)), shape=(num_rows, num_cols) )
print adj.shape
print ""

print "done importing in to sparse matrix", time.time() - wall, " and ", time.clock() - clock;

print "getting subreddit counts"
wall,clock = time.time(),time.clock()

# now we have our matrix, so let's gather up a bit of info about it
users_per_subreddit = adj.sum(axis=1).A1
subreddits = range(len(subreddit_to_id))
for sr in subreddit_to_id:
    subreddits[subreddit_to_id[sr]] = sr
subreddits = np.array(subreddits)

print "done getting subreddit counts", time.time() - wall, " and ", time.clock() - clock;

from sklearn.decomposition import TruncatedSVD 
from sklearn.preprocessing import normalize 

print "doing truncated SVD"
wall,clock = time.time(),time.clock()

svd = TruncatedSVD(n_components=100)
svd_output = svd.fit_transform(adj)
print svd_output.shape

print "done truncated SVD", time.time() - wall, " and ", time.clock() - clock;

print "normalizing"
wall,clock = time.time(),time.clock()
embedded_coords = normalize(svd_output, norm='l1')
print "done normalizing", time.time() - wall, " and ", time.clock() - clock;
print embedded_coords.shape

from scipy.stats import rankdata
embedded_ranks = np.array([rankdata(c) for c in embedded_coords.T]).T

print "doing kmeans"
wall,clock = time.time(),time.clock()
from sklearn.cluster import KMeans
n_clusters = 20
km = KMeans(n_clusters)
clusters = km.fit_predict(embedded_ranks)
print "done kmeans", time.time() - wall, " and ", time.clock() - clock;

print "doing t-SNE"
wall,clock = time.time(),time.clock()
from sklearn.manifold import TSNE
row_selector = np.where(users_per_subreddit>100)
xycoords = TSNE().fit_transform(embedded_coords[row_selector])

print "done t-SNE", time.time() - wall, " and ", time.clock() - clock;

