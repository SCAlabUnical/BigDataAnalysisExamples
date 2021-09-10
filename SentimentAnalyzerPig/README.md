The Pig application discussed here shows how to use Pig for implementing a
dictionary-based sentiment analyzer. Since Pig does not provide a built-in library
for sentiment analysis, the system exploits external dictionaries to associate words
to their sentiments and determine the semantic orientation of the opinion words.
Given a dictionary of words associated with positive or negative sentiment, the sen-
timent of a text (e.g., sentence, review, tweet, comment) is calculated by summing
the scores of positive and negative words in the text and then by calculating the
average rating.

Developers can include advanced analytics in a script by defining
UDFs. For example, the PROCESS UDF is aimed at processing a
tuple by removing punctuation as a preprocessing step. Other functionalities, if
required, can be added to the exec method, which is implemented in Java.

Once registering the UDF defined in Java, the dataset containing the reviews is loaded from HDFS.
Each review is tokenized and processed according to the function defined above. 
Then, a score from a sentiment dictionary is assigned to each token and the
final rating of a review is calculated as the average of the scores of its tokens.
