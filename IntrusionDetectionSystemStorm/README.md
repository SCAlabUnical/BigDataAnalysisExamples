The proposed application shows how to leverage Storm for implementing a network
intrusion detection system. Network intrusion detection is a critical part of network management for security and quality of service. 
These systems allow early detection of network intrusion and malicious activities through anomaly detection based techniques (AIDS, Anomaly-based Intrusion Detection System).
A Storm application requires defining three entities: spouts, bolts and the topology. The proposed topology, composed of one spout and two bolts, is given below:
- ConnectionSpout is the only data source. This spout streams connections coming from a firewall or stored in a log file, 
  and each record is forwarded as a tuple to the next bolt. In this example, a connection is described by 41 features,
  some of which are duration, protocol type, service, etc.;
- DataPreprocessingBolt receives the tuples from the spout and performs preprocessing. 
  Specifically, it converts the categorical features to numerical and
 performs standardization for the machine learning model;
- ModelBolt performs the classification through a Support Vector Machine
(SVM) model trained offline and stores the results to a file for further analysis.
The training phase is performed offline using the Python
scikit-learn library, as Storm does not provide any native machine learning library.
All the trained models (i.e., standard scaler for numerical features, label encoder for categorical features and the SVM model)
are dumped in files using the pickle module. Hence, the Storm Multi-Language protocol can be adopted to use the
trained models in a topology implemented in a JVM language.

A topology can be submitted to a production cluster using the storm client, specifying the path of the jar file, the class-name to run, and any other arguments. The shuffle grouping ensures that tuples are randomly distributed so that each bolt receives an equal number of tuples.

The data model used by Storm is the tuple. Each spout node must specify the collector used to emit the tuples (method open), how to emit the next tuple (method nextTuple) and must declare the output fields for the tuples it emits (method declareOutputFields).

Each tuple is emitted by the spout and will be processed by the subsequent
bolts as declared in the topology. In this case, the class DataPreprocessingBolt is a proxy for the Python bolt, which processes the tuples by applying the transformations of a set of trained models loaded from the disk (e.g., the encoders for categorical features and the scalers for numerical features). The Multi-Language protocol only requires the bolt specifies the script to execute, while all the application logic is contained in the Python script.

Finally, the ModelBolt acts similarly to the DataPreprocessingBolt, with the application of the SVM model trained offline using the scikit-learn library.
The predicted connection type, from a set of 23 types (e.g., smurf, buffer overflow, guess password, etc.), allows the Network Security infrastructure to react and
mitigate possible threats.
