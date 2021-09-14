The proposed application shows how to implement a RoI Mining application with
Hive. The widespread use of social media and location-based services makes it possible 
to extract very useful information for understanding the behavior of large
groups of people. Every day millions of people log into social media and share information
about the places they visit. The analysis of geo-referenced data produced
by users on social media is useful for determining whether users have visited interesting
places (e.g., tourist attractions, shopping centres, squares, parks), often
called Places-of-Interest (PoIs). Since a PoI is generally identified by the geographic
coordinates of a single point, it is useful to define a Region-of-Interest (RoI), an
area represented by the geographic boundaries of the PoI. RoI Mining techniques
are aimed precisely at the discovery of regions of interest.
In this application data is collected from Flickr, a social network used for sharing photos.
The initial goal is to assign a generic geo-localized Flickr post to the
corresponding PoI, through an analysis of the textual content and metadata of the
post (tags and description). After assigning each post to its PoI, the geographical
coordinates <longitude;latitude> are aggregated through DBSCAN (Density-Based Spatial Clustering of
Applications with Noise), a density-based clustering algorithm
that exploits information on the density of points to identify clusters that are representative
of a RoI. The cluster with the largest size represents the most significant
subregion of the PoI to which it belongs since it is characterized by a greater density
of the 2D points identified by the pair of geographical coordinates. 

Flickr data is firstly filtered to select only a few fields for analysis: the latitude
and longitude contain information related to geographical coordinates; description
contains a description of the post; dateposted contains the date it was posted; username 
contains the id of the user who shared the post and tags is a string that
contains the set of tags, separated by commas, which give additional information.

The first step is to map a Flickr tuple to the corresponding PoI. It can be done
by defining an UDF that allows to run custom Java code within a Hive script;
in particular, a Regular UDF works on a single row of a table and produces a
single output row. The assignRoI method checks if the tags and the
description contain a keyword of the file loaded as input and returns the name of
the RoI (e.g., the Colosseum is also called the Flavian Amphitheater, Coliseo, etc.).

The function can be called in a Hive query for determining
a ranking of the most visited RoIs. In particular, the GeoData function in the select
clause returns the name of a RoI for each point and the number of users who have
visited that area. The selected rows are grouped by the RoI's name (group by clause)
and sorted by the number of visitors (order by clause).

At this point data is ready for clustering analysis. To launch the DBSCAN algorithm
it is required the implementation of an UDAF, which applies a function to
multiple rows of a table by implementing five methods:
- init(): it initializes the evaluator, which actually implements the UDAF logic;
- iterate(): it is called whenever there is a new value to aggregate;
- terminatePartial(): it is called for the partial aggregation, and returns an
object that encapsulates the state of the aggregation;
- merge(): it is called to combine a partial aggregation with another;
- terminate(): it is called when the final result of the aggregation is required.
In particular, DBSCAN is launched on the points belonging to the same RoI,
where the name of a RoI is obtained using the previously defined GeoData function.
Since DBSCAN should find more than one cluster, the one
containing the highest number of points is chosen and returned as a KML (Keyhole
Markup Language) string.

Finally, the UDAF can be called in the Hive script to get the final results.
