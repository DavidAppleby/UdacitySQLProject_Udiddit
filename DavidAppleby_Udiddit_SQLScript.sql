/*
    Part 1 - DDL for new, normalized schema
*/

CREATE TABLE users
(
  id BIGSERIAL CONSTRAINT "users_pk" PRIMARY KEY,
  username VARCHAR(25) CONSTRAINT "username_not_null" NOT NULL,
  last_login TIMESTAMP,
  created_date TIMESTAMP,
  created_by VARCHAR(25),
  updated_date TIMESTAMP,
  updated_by VARCHAR(25),
  CONSTRAINT "empty_usernames_not_allowed" CHECK (LENGTH(TRIM("username")) > 0)
);

CREATE UNIQUE INDEX "unique_username" ON "users"(TRIM("username"));

CREATE TABLE topics
(
  id BIGSERIAL CONSTRAINT "topics_pk" PRIMARY KEY,
  user_id BIGINT DEFAULT NULL,
  name VARCHAR(30) CONSTRAINT "topic_name_not_null" NOT NULL,
  description VARCHAR(500) DEFAULT NULL,
  created_date TIMESTAMP,
  created_by VARCHAR(25),
  updated_date TIMESTAMP,
  updated_by VARCHAR(25),
  CONSTRAINT "empty_names_not_allowed" CHECK (LENGTH(TRIM("name")) > 0)
);

CREATE UNIQUE INDEX "unique_topic" ON "topics"(TRIM("name"));
CREATE INDEX ON "topics"(LOWER("name") VARCHAR_PATTERN_OPS);

CREATE TABLE posts
(
  id BIGSERIAL CONSTRAINT "posts_pk" PRIMARY KEY,
  author_id BIGINT,
  topic_id BIGINT CONSTRAINT "topic_required" NOT NULL,
  title VARCHAR(100) CONSTRAINT "title_not_null" NOT NULL,
  url VARCHAR(500) DEFAULT NULL,
  text_content VARCHAR(1500) DEFAULT NULL,
  created_date TIMESTAMP,
  created_by VARCHAR(25),
  updated_date TIMESTAMP,
  updated_by VARCHAR(25),
  CONSTRAINT "empty_titles_not_allowed" CHECK (LENGTH(TRIM("title")) > 0),
  CONSTRAINT "url_or_txtContent" CHECK (
    (NULLIF(url,'') IS NULL OR NULLIF(text_content,'') IS NULL)
    AND NOT  
    (NULLIF(url,'') IS NULL AND NULLIF(text_content,'') IS NULL)
  ),
  CONSTRAINT "posts_to_topics_fk" FOREIGN KEY ("topic_id") REFERENCES "topics" ON DELETE CASCADE,
  CONSTRAINT "posts_to_users_fk" FOREIGN KEY ("author_id") REFERENCES "users" ON DELETE SET NULL
);

CREATE INDEX "posts_by_user" ON "posts"("author_id");
CREATE INDEX "topics_post_matching" ON "posts"("topic_id");
CREATE INDEX "find_post_with_url" ON "posts"("url");


CREATE TABLE comments
(
  id BIGSERIAL CONSTRAINT "comments_pk" PRIMARY KEY,
  author_id BIGINT,
  post_id BIGINT,
  text_content VARCHAR(1500) CONSTRAINT "comment_not_null" NOT NULL,
  parent_id BIGINT DEFAULT NULL,
  created_date TIMESTAMP,
  created_by VARCHAR(25),
  updated_date TIMESTAMP,
  updated_by VARCHAR(25),
  CONSTRAINT "empty_comments_not_allowed" CHECK (LENGTH(TRIM("text_content"))>0),
  CONSTRAINT "parent_child_comments_fk" FOREIGN KEY ("parent_id") REFERENCES "comments" ON DELETE CASCADE,
  CONSTRAINT "comment_to_post_fk" FOREIGN KEY ("post_id") REFERENCES "posts" ON DELETE CASCADE,
  CONSTRAINT "comment_to_users_fk" FOREIGN KEY ("author_id") REFERENCES "users" ON DELETE SET NULL
);

CREATE INDEX "find_parent_comments" ON "comments"("parent_id");
CREATE INDEX "find_comments_by_user" ON "comments"("author_id");

CREATE TABLE user_votes
(
  user_id BIGINT,
  post_id BIGINT,
  vote SMALLINT CONSTRAINT "valid_votes" CHECK ("vote" IN (-1, 1)),
  created_date TIMESTAMP,
  created_by VARCHAR(25),
  updated_date TIMESTAMP,
  updated_by VARCHAR(25),
  CONSTRAINT "user_votes_pk" PRIMARY KEY ("post_id","user_id"),
  CONSTRAINT "votes_to_users_fk" FOREIGN KEY ("user_id") REFERENCES "users" ON DELETE SET NULL,
  CONSTRAINT "votes_to_posts_fk" FOREIGN KEY ("post_id") REFERENCES "posts" ON DELETE CASCADE
);

CREATE INDEX "compute_votes" ON "user_votes"("vote");

/*
    Part 2 - Data Migration
*/

/* Step 1 - Migrate data to "users" table 
   I used "UNION" to remove all duplicates. */

INSERT INTO "users"("username")
  SELECT username
  FROM   bad_posts

  UNION

  SELECT regexp_split_to_table(upvotes, ',')::VARCHAR AS username
  FROM   bad_posts

  UNION

  SELECT regexp_split_to_table(downvotes, ',')::VARCHAR AS username
  FROM   bad_posts 

  UNION

  SELECT (username::VARCHAR) AS username
  FROM   bad_comments;

/* Step 2 - Migrate data to "topics" table 
   "DISTINCT" used to remove duplicates */

INSERT INTO "topics"("name")
  SELECT DISTINCT topic
  FROM bad_posts;

/* Step 3 - Migrate data to "posts" table */

INSERT INTO "posts"("id","author_id","topic_id","title","url","text_content")
  SELECT  bp.id::BIGINT AS posts_pk,
          u.id AS author_id,
          t.id AS topic_id,
          bp.title::VARCHAR(100),
          bp.url::VARCHAR(500),
          bp.text_content::VARCHAR
  FROM topics t
       INNER JOIN
       bad_posts bp
       ON 
       t.name = bp.topic
       INNER JOIN
       users u
       ON
       bp.username = u.username; 

/* Step 4 - Migrate data to "comments" table */

INSERT INTO "comments"("author_id","post_id","text_content")
  SELECT  u.id AS author_id,
          bc.post_id,
          bc.text_content::VARCHAR(1500)
  FROM users u
       INNER JOIN
       bad_comments bc
       ON u.username = bc.username; 

/* Step 5 - Migrate data to "user_votes" table */
INSERT INTO "user_votes"("user_id","post_id", "vote")
  SELECT  t2.user_id,
          t1.post_id,
          t1.vote
  FROM (
          (
            SELECT  bp.id::BIGINT AS post_id,
                    regexp_split_to_table(bp.upvotes,',')::VARCHAR AS username,
                    1::SMALLINT AS vote
            FROM    bad_posts bp
          ) AS t1
          INNER JOIN
          (
            SELECT  u.id AS user_id,
                    u.username
            FROM users u      
          ) AS t2
          ON t1.username = t2.username         
       )

  UNION ALL

  SELECT  t4.user_id,
          t3.post_id,
          t3.vote
  FROM (
          (
            SELECT  bp.id::BIGINT AS post_id,
                    regexp_split_to_table(bp.downvotes,',')::VARCHAR AS username,
                    -1::SMALLINT AS vote
            FROM    bad_posts bp
          ) AS t3
          INNER JOIN
          (
            SELECT  u.id AS user_id,
                    u.username
            FROM users u      
          )AS t4
          ON t3.username = t4.username
);


/* 
  A. List all users who haven't logged in the last year.
*/

SELECT  username 
FROM    users
WHERE   last_login < NOW() - '1 year'::INTERVAL;

/*
  B. List all users who haven't created any post.
*/

SELECT  u.username  
FROM    users u  
        LEFT JOIN 
        posts p  
        ON u.id = p.author_id
WHERE   p.author_id IS NULL;

/*      
  C. Find a user by their username.  
*/

SELECT *
FROM users   
WHERE username = 'Zula71';

/*
  D. List all topics that don't have any posts.  
*/

SELECT  t.name  
FROM    topics t
        LEFT JOIN
        posts p  
        ON t.id = p.topic_id
WHERE   p.topic_id IS NULL;

/*
  E. Find a topic by its name.   
*/

SELECT  *
FROM    topics
WHERE   name = 'Beauty';

/*
  F. List the latest 20 posts for a given topic
*/

SELECT  t.name,
        p.title,
        u.username AS author,
        p.url,
        p.text_content
FROM    topics t  
        INNER JOIN 
        posts p  
        ON t.id = p.topic_id
        INNER JOIN 
        users u  
        ON u.id = p.author_id
WHERE   t.name = 'calculate'
ORDER BY  p.created_date DESC
LIMIT 20;

/*
  G.  List the latest 20 posts for a given user
*/

SELECT  p.title,
        u.username AS author,
        p.url,
        p.text_content
FROM    posts p    
        INNER JOIN  
        users u  
        ON u.id = p.author_id AND u.username = 'Dora55'
ORDER BY p.created_date DESC
LIMIT 20;

/*
  H. Final all posts that link to a specific URL, for moderation purposes.  
*/

SELECT p.id,
       p.title,
       u.username AS author,
       u.id AS user_id,
       p.url
FROM   posts p  
       INNER JOIN  
       users u  
       ON u.id = p.author_id  AND  p.url = 'http://vivien.org'
ORDER BY p.id ASC;

/*
  I. List all the top-level comments (those that don't have a parent comment) for a given post. 
*/

SELECT  p.title post,
        u.username AS author,
        c.text_content
FROM    posts p   
        INNER JOIN
        comments c   
        ON p.id = c.post_id AND c.parent_id IS NULL AND p.id = 10000
        INNER JOIN 
        users u  
        ON u.id = c.author_id;

/*
  J. List all the direct children of a parent comment.
*/

SELECT  u.username AS author,
        parent.id AS parent_comment_id,
        child.id AS child_comment_id,
        child.created_date AS comment_date,
        child.text_content
FROM    comments parent
        INNER JOIN  
        comments child 
        ON child.parent_id = parent.id AND parent.id = 1
        INNER JOIN 
        users u 
        ON child.author_id = u.id
ORDER BY 4 ASC;

/*
  K.  List the latest 20 comments made by a given user.  
*/

SELECT  u.username AS author,
        c.text_content
FROM    comments c  
        INNER JOIN 
        users u  
        ON c.author_id = u.id AND u.username = 'Kolby.Langosh'
ORDER BY c.created_date DESC
LIMIT 20;

/*
  L. Compute the score of a post, defined as the difference between the number of 
     upvotes and the number of downvotes.
*/

SELECT  p.title,
        SUM(uv.vote) AS post_score
FROM    posts p  
        INNER JOIN 
        user_votes uv  
        ON p.id = uv.post_id
GROUP BY p.title  
ORDER BY 2 DESC, 1 ASC; 





        







