The proposed application shows how to exploit Spark for implementing a customer
churn prediction system (i.e., customer switch from a company to another).
In order to identify potential churners and re-engage them, machine learning algorithms can help to mine shared behavioral patterns of those already churned
customers and promptly detect current customers at risk of churn. Due to the volume of historical data of both churned customers and existing ones, and the need to periodically analyze new customers and retrain the model, distributed and parallel frameworks such as Spark can be employed with benefits.
Spark is used for preprocessing historical data and training a prediction model, which will be used to forecast whether a customer will change to another company. Specifically, the MLlib package is employed to handle data organized in RDDs (Resilient Distributed Datasets) and build the classification model.

The dataset used for training the prediction model consists of telecommunication
customer activity data (e.g., total day minutes, total day calls, customer service
calls, etc.), along with a churn label specifying whether a customer has cancelled
the subscription. Overall, a generic tuple in the dataset is composed of 20 features.
Once connected to the master node of the Spark cluster via the spark session, data
is retrieved from a batch file and uploaded into a RDD, as shown by the Scala code.
The objects representing the different users are defined by parsing the
RDD. Then, data is cached for performance purposes.

Afterwards, data is processed to be properly used by the machine learning algorithm. 
A tuple is converted to a LabeledPoint, which represents the features (as a
local dense vector) and the label of a data point. Categorical features are encoded as numerical to be standardized by removing the mean and scaling to unit variance.
The numericalFeature function exploits a map to assign a numerical value (1:0 or 0:0) to categorical features.

To train the model, the preprocessed data is split into training and test set (70%
and 30% respectively) and a decision tree model is configured with three main
hyper-parameters: i) the impurity measure used to compute the information gain of
a split; ii) the maximum depth for terminating the algorithm; and iii) the maximum
number of bins used when discretizing continuous features.
After training, the model is saved to disk for later classification of existing and unclassified customers. Before that, the test set is used to evaluate the model against unseen examples by computing the test error and binary metrics such as precision and recall. Empty categoricalFeatures indicates that all features are continuous after the scaling. 

The classification model can be exploited to periodically monitor current customers to find potential churners and react appropriately. The system integrates
structured data from a data warehouse, such as Apache Hive, a feature extraction module and the trained model to infer new churning customers. Queries to Hive warehouse are expressed in HiveQL.
