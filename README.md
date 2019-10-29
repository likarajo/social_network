# Social Network

Created a social network of users liking each other's posts. For every person, we keep track of their name and age.
Every edge will contain source, destination and an integer indicating the number of times a person liked other's posts.

<div align="center">
  <img src="social_network.png" width=600 />
</div>  

Used Spark RDD and GraphX to find out the following informations:

1. Create a listing of those people who are at least 30 years old
2. Find and display the number of like combinations in the graph
3. Create a listing of the likes (who likes who's posts)
4. Create a listing of those triplets where the number of likes is more than 5
5. Find and display the indegrees of each vertex
6. Find the user that follows the most number of people
7. Find how many people are there whose posts user 5 likes, and also like each other's posts
8. Find the oldest follower of each user
9. Find the youngest follower of each user
10. Find the youngest person that each person is following
11. Find the pagerank of each user and list them in descending order
