# To implement in Cheetah DB:
- Add as high priority reference the cluster-ready algorithms structure (the tree-fork data approach could work in a distributed scenario)
- Implement a "matrix-related" tree-series predictions (Go to "Matrix related tree")

## Matrix-related tree
Cheetah uses trees structure to divide bytes in well-defined boundaries fast access files. Here's leans the possibility to extend its basic usage for prediction purposes.
But predictions are not dynamic if there is no way to relate probabilities to a (dynamic) context matrix.
Here is needed to define a particular type of table: the prediction (with context capability). 
A prediction tables:
- Can returns multiples results for the same series of bytes
- Can evaluate multiple windows of a byte array in base of query options
- Is able to change probabilities in base of current reference context, representend by a "matrix" (that sub-arrays can have different sizes)
- Discard low-probability connections: especially during training, first connections are "lousy", and storing all connections would occupy gigabytes of storage. A real time command/automatic limit for discarding irrilevant connection is needed. 

In case of prediction table, a value of a key, instead of referencing to a value, references to a series of probabilities. More likely with GPU computation in machine learning, and is important to evaluate is possible to use WebGPU-like libraries for cross-platform accellerated computation, it could be requested by command query (or table settings) to returns a complex prediction merging. A basic example is merging probabilities using multiple key-window at the same time, truncanting the probability of tot bits each steps to the requested bytes-key and then merging the resulting probability. Similar concept must applied for context. A context is an "array of array", where every sub-array can have a different size, containing angular value where, in combination when the other ones, changes the probability weights of results. This requires a statistical-machine learning like algorithm (and related "training" phase while adding/editing) that gives to each results a weight/bias dependency to each vector in the array. The sub-array, in reality, has "infinite values", not specified values count as "value setted to 0", and too additional arrays to main array count as "deeper context adding", in case of more precision is needed, always if current results has weights that depends of them given during training phase, or they're un-affacted. This weights-relation changes probabilities in a given way (bias) also in correlation with other weights in the sub-array, and this requires some cycles of forward-backward weights-apply adjustments to obtain right combination, while, instead, every sub-array can be applied by their description order: from top to the deeper, so the deepers may be optionals and the top doesn't depends on the lower one, allowing also forms of fine-tuning. 
It's important that results are stored by their values to not be too dispersive, these value contains every "weights dependency", where in a very similar and recursive way in its algorithm, weights that alters prediction probabilities are altered also by the current series of bytes that gave these results, and probably in some case also the other related results. 

Remember a standolone implementation, but while studying new ideal Cheetah's algorithms, tables, query commands etc, take as reference main project for further implementation of new cheetah's features in it.

### Last things
- Implement in lmdb python code src/ the new Cheetah DB's prediction-tables and context-matrix features and queries 