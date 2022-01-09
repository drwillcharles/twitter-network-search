// Load all data from CSV files into Neo4j database

// If this is the first time setting up the database, may need to create constraints
CREATE CONSTRAINT user_id ON (u:User) ASSERT u.userID IS UNIQUE;
CREATE CONSTRAINT tweet_id ON (t:Tweet) ASSERT t.tweetID IS UNIQUE;
CALL db.awaitIndexes();

// Create Users
LOAD CSV WITH HEADERS FROM 'file:///df_users.csv' AS row
MERGE (user:User {userID: row.id})
  ON CREATE SET user.name = row.name,
  user.userName = row.username;

// Create Tweets 
LOAD CSV WITH HEADERS FROM 'file:///df_tweets.csv' AS row
MERGE (tweet:Tweet {tweetID: row.id})
  ON CREATE SET tweet.authorID = row.author_id,
  tweet.text = row.text,
  tweet.like_count = row.like_count,
  tweet.created_at = row.created_at;
  
// Create relationships between Tweets and Creators
LOAD CSV WITH HEADERS FROM 'file:///df_tweets.csv' AS row
MATCH (user:User {userID: row.author_id})
MATCH (tweet:Tweet {tweetID: row.id})
MERGE (user)-[op:CREATED]->(tweet);

//Create 'Replied_To' relationship
LOAD CSV WITH HEADERS FROM 'file:///df_tweets.csv' AS row
MATCH (tweet_from:Tweet {tweetID: row.id})
MATCH (tweet_to:Tweet {tweetID: row.referenced_tweet_id})
WHERE row.type = "replied_to"
MERGE (tweet_from)-[op:REPLIED_TO]->(tweet_to);

//Create 'Quoted' relationship (may not exist)
LOAD CSV WITH HEADERS FROM 'file:///df_tweets.csv' AS row
MATCH (tweet_from:Tweet {tweetID: row.id})
MATCH (tweet_to:Tweet {tweetID: row.referenced_tweet_id})
WHERE row.type = "quoted"
MERGE (tweet_from)-[op:QUOTED]->(tweet_to);
