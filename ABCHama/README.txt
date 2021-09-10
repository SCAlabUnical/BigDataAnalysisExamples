The proposed application shows how to exploit Hama for addressing the in
uence maximization problem. Since social media platforms are increasingly used to convey advertising campaigns for products or services, the goal is to identify a set of k users in a social network, namely seeds, that maximizes the spread (i.e., the number of influenced users). The application is based on a bio-inspired technique, called the Artificial Bee Colony, and belongs to the field of swarm intelligence that focuses on the study of self-organized systems in which a complex action derives from a collective intelligence. The ABC system is composed of three main entities:
• Food source, characterized by its goodness in terms of quantity of nectar or
distance from the hive;
• Employer bees, which collect the nectar and carry details about the source of
food to the hive;
• Unemployer bees, which are not currently picking up nectar. They can be
divided into two categories: scout bees, which search for new sources of food
and on-looker bees, which choose a source of food according to the information
brought to the hive by the employer bees.
The main goal of the system is to maximize the nectar collection and it can
be adapted to the influence maximization problem. Specifically, each user of a social network is considered as a source of food, employer bees identify the opinion leaders of the network (i.e., final seeds), scout bees are used for exploring the neighborhood of employer bees, and on-looker bees indicate the influenced users.

The main step involved in writing a Hama graph application is to extend the predefined Vertex class, specifying the value types for vertices, edges and messages through its template arguments. The user must encode, by overriding the compute() method, the behavior of a vertex, i.e. the set of operations that will be executed by each active node at each superstep. Furthermore, built-in methods such as sendMessage(Edge <V, E> e, M msg) and getValue() allow the current vertex to send messages to other vertices or to inspect its associated value. During the setup phase the nodes initialize their data structures and one of them is elected as master, taking on the role of coordinator. The compute method separates the behavior of the master from that of the other vertices, by simply checking the id associated with the current vertex.

The behavior of the vertices depends on the type of message
they receive. During the first phase each seed sends the rank of its neighbors to
the master. Then, when notified by the master, the vertex sends a new message to
its neighbors specifying the activation probability. Once the propagation phase is
over, that is there are no more messages to be processed, the node with maximum
influence probability is chosen, sending this value to an aggregator that evaluates the fitness of each seed. Finally, when a vertex receives the stop signal from the master, it votes to halt the execution and suspend itself.

The behavior of the master, shown in Listing 16, is described as follows. Once
the first iterative phase is over, it elects the scout bees with the highest ranking, notifying the beginning of the influence evaluation. Thus, each scout bee sends an influence message along the outgoing edges, in order to evaluate its finess. Once the aggregator has completed the evaluation, the master determines whether to proceed with the role switch (scout -> employer), communicating it to the other nodes. The process iterates until either the entire set of scout bees is evaluated or convergence is reached (i.e., the minimum percentage increment of the spread between two subsequent iterations is less than a threshold w). At the end of the process the final result is stored, which consists of the final seed set (i.e., the selected influencers) and the expected spread of influence within the network.