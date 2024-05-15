import pandas as pd
import numpy as np
import sqlite3

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

def solution_1(Posts, Users):
    """
    Executes SQL and Pandas operations to find the top 10 locations by post count where location is not empty.

    Parameters:
    - Posts (pd.DataFrame): DataFrame containing posts data.
    - Users (pd.DataFrame): DataFrame containing users data.

    Returns:
    - Tuple of DataFrames: (sql_result, pandas_result)
    """
    # Creating an SQLite in-memory database connection
    conn = sqlite3.connect(':memory:') 

    # Saving DataFrames to SQL
    Users.to_sql('Users', conn, index=False, if_exists='replace')
    Posts.to_sql('Posts', conn, index=False, if_exists='replace')

    # SQL query
    sql_query = """
    SELECT Location, COUNT(*) as Count
    FROM Users JOIN Posts ON Users.Id = Posts.OwnerUserId
    WHERE Location != ''
    GROUP BY Location
    ORDER BY Count DESC
    LIMIT 10
    """
    sql_result = pd.read_sql_query(sql_query, conn)

    # Equivalent Pandas query
    pandas_result = (Users.merge(Posts, left_on='Id', right_on='OwnerUserId')
                     .query("Location != ''")
                     .groupby('Location')
                     .size()
                     .reset_index(name='Count')
                     .sort_values(by='Count', ascending=False)
                     .head(10))

    # Resetting the index in the Pandas DataFrame
    pandas_result.reset_index(drop=True, inplace=True)

    # Closing the database connection
    conn.close()

    return sql_result, pandas_result

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

def solution_2(Posts, PostLinks):
    """
    Executes SQL and Pandas operations to find the titles of posts and the number of related posts (links),
    where the post is a question (PostTypeId = 1) and sorts the result first by the number of links in descending order
    and then alphabetically by title where numbers of links are equal.

    Parameters:
    - Posts (pd.DataFrame): DataFrame containing posts data.
    - PostLinks (pd.DataFrame): DataFrame containing post links data.

    Returns:
    - Tuple of DataFrames: (sql_result, pandas_result)
    """
    # Creating an SQLite in-memory database connection
    conn = sqlite3.connect(':memory:') 

    # Saving DataFrames to SQL
    Posts.to_sql('Posts', conn, index=False, if_exists='replace')
    PostLinks.to_sql('PostLinks', conn, index=False, if_exists='replace')

    # SQL query
    sql_query = """
    SELECT Posts.Title, RelatedTab.NumLinks
    FROM (
        SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks
        FROM PostLinks
        GROUP BY RelatedPostId
    ) AS RelatedTab
    JOIN Posts ON RelatedTab.PostId = Posts.Id
    WHERE Posts.PostTypeId = 1
    ORDER BY NumLinks DESC, Title ASC
    """
    sql_result = pd.read_sql_query(sql_query, conn)

    # Equivalent Pandas query
    post_links_count = (PostLinks.groupby('RelatedPostId')
                                  .size()
                                  .reset_index(name='NumLinks'))
    pandas_result = (post_links_count.merge(Posts, left_on='RelatedPostId', right_on='Id')
                                     .query("PostTypeId == 1")
                                     .loc[:, ['Title', 'NumLinks']]
                                     .sort_values(by=['NumLinks', 'Title'], ascending=[False, True])
                                     .reset_index(drop=True))  

    # Closing the database connection
    conn.close()

    return sql_result, pandas_result

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

def solution_3(Comments, Posts, Users):
    """
    Executes SQL and Pandas operations to find the top 10 posts by total comments score 
    with details like title, comment count, view count, display name, reputation, and location.

    Parameters:
    - Comments (pd.DataFrame): DataFrame containing comments data.
    - Posts (pd.DataFrame): DataFrame containing posts data.
    - Users (pd.DataFrame): DataFrame containing users data.

    Returns:
    - Tuple of DataFrames: (sql_result, pandas_result)
    """
    # Creating an SQLite in-memory database connection
    conn = sqlite3.connect(':memory:')

    # Saving DataFrames to SQL
    Users.to_sql('Users', conn, index=False, if_exists='replace')
    Posts.to_sql('Posts', conn, index=False, if_exists='replace')
    Comments.to_sql('Comments', conn, index=False, if_exists='replace')

    # SQL query
    sql_query = """
    SELECT p.Title, p.CommentCount, p.ViewCount, c.CommentsTotalScore, u.DisplayName, u.Reputation, u.Location
    FROM (
        SELECT PostId, SUM(Score) AS CommentsTotalScore
        FROM Comments
        GROUP BY PostId
    ) AS c
    JOIN Posts p ON p.Id = c.PostId
    JOIN Users u ON u.Id = p.OwnerUserId
    WHERE p.PostTypeId = 1
    ORDER BY c.CommentsTotalScore DESC
    LIMIT 10
    """
    sql_result = pd.read_sql_query(sql_query, conn)

    # Equivalent Pandas query
    Comments_agg = Comments.groupby('PostId').agg(CommentsTotalScore=('Score', 'sum')).reset_index()
    pandas_result = (Posts.merge(Comments_agg, left_on='Id', right_on='PostId', how='inner')
                          .merge(Users, left_on='OwnerUserId', right_on='Id', how='inner')
                          .query("PostTypeId == 1")
                          .sort_values(by='CommentsTotalScore', ascending=False)
                          .head(10)
                          .loc[:, ['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore', 'DisplayName', 'Reputation', 'Location']])

    pandas_result.reset_index(drop=True, inplace=True)

    # Closing the database connection
    conn.close()

    return sql_result, pandas_result
    
# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

def solution_4(Posts, Users):
    """
    Executes SQL and Pandas operations to identify users with more answers than questions,
    order them by the number of answers, and limit the output to the top 5.

    Parameters:
    - Posts (pd.DataFrame): DataFrame containing posts data.
    - Users (pd.DataFrame): DataFrame containing user data.

    Returns:
    - Tuple of DataFrames: (sql_result, pandas_result)
    """
    # Creating an SQLite in-memory database connection
    conn = sqlite3.connect(':memory:')  

    # Saving DataFrame to SQL
    Users.to_sql('Users', conn, index=False, if_exists='replace')
    Posts.to_sql('Posts', conn, index=False, if_exists='replace')

    # SQL query
    sql_query = """
    SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
    FROM (
        SELECT Users.DisplayName, QuestionsNumber, AnswersNumber, Users.Location, Users.Reputation, Users.UpVotes, Users.DownVotes
        FROM (
            SELECT OwnerUserId, COUNT(*) AS AnswersNumber
            FROM Posts
            WHERE PostTypeId = 2
            GROUP BY OwnerUserId
        ) AS Answers
        JOIN (
            SELECT OwnerUserId, COUNT(*) AS QuestionsNumber
            FROM Posts
            WHERE PostTypeId = 1
            GROUP BY OwnerUserId
        ) AS Questions ON Answers.OwnerUserId = Questions.OwnerUserId
        JOIN Users ON Users.Id = Answers.OwnerUserId
        WHERE AnswersNumber > QuestionsNumber
        ORDER BY AnswersNumber DESC
        LIMIT 5
    )
    """
    sql_result = pd.read_sql_query(sql_query, conn)

    # Equivalent Pandas query
    answers = Posts[Posts['PostTypeId'] == 2].groupby('OwnerUserId').size().reset_index(name='AnswersNumber')
    questions = Posts[Posts['PostTypeId'] == 1].groupby('OwnerUserId').size().reset_index(name='QuestionsNumber')
    combined = answers.merge(questions, on='OwnerUserId')
    combined = combined[combined['AnswersNumber'] > combined['QuestionsNumber']]
    combined = combined.sort_values(by='AnswersNumber', ascending=False).head(5)
    pandas_result = combined.merge(Users, left_on='OwnerUserId', right_on='Id')
    pandas_result = pandas_result[['DisplayName', 'QuestionsNumber', 'AnswersNumber', 'Location', 'Reputation', 'UpVotes', 'DownVotes']]

    # Closing the database connection
    conn.close()

    return sql_result, pandas_result
    
# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

def solution_5(Posts, Users):
    """
    Executes SQL and Pandas operations to find the top 10 users by average answers count per post.

    Parameters:
    - Posts (pd.DataFrame): DataFrame containing posts data.
    - Users (pd.DataFrame): DataFrame containing users data.

    Returns:
    - Tuple of DataFrames: (sql_result, pandas_result)
    """
    # Creating an SQLite in-memory database connection
    conn = sqlite3.connect(':memory:')  

    # Saving DataFrames to SQL
    Users.to_sql('Users', conn, index=False, if_exists='replace')
    Posts.to_sql('Posts', conn, index=False, if_exists='replace')

    # SQL query
    sql_query = """
    SELECT Users.AccountId, Users.DisplayName, Users.Location, ROUND(AVG(PostAuth.AnswersCount), 1) as AverageAnswersCount
    FROM (
        SELECT AnsCount.AnswersCount, Posts.Id, Posts.OwnerUserId
        FROM (
            SELECT Posts.ParentId, COUNT(*) AS AnswersCount
            FROM Posts
            WHERE Posts.PostTypeId = 2
            GROUP BY Posts.ParentId
        ) AS AnsCount
        JOIN Posts ON Posts.Id = AnsCount.ParentId
    ) AS PostAuth
    JOIN Users ON Users.AccountId = PostAuth.OwnerUserId
    GROUP BY OwnerUserId
    ORDER BY AverageAnswersCount DESC, AccountId
    LIMIT 10
    """
    sql_result = pd.read_sql_query(sql_query, conn)

    # Equivalent Pandas query
    answer_counts = Posts[Posts['PostTypeId'] == 2].groupby('ParentId')['Id'].count().reset_index(name='AnswersCount')
    post_auth = Posts.merge(answer_counts, left_on='Id', right_on='ParentId')
    post_auth = post_auth[['OwnerUserId', 'AnswersCount']]
    avg_answers = post_auth.groupby('OwnerUserId')['AnswersCount'].mean().reset_index(name='AverageAnswersCount')
    avg_answers['AverageAnswersCount'] = avg_answers['AverageAnswersCount'].round(1)
    pandas_result = Users.merge(avg_answers, left_on='AccountId', right_on='OwnerUserId')[['AccountId', 'DisplayName', 'Location', 'AverageAnswersCount']]
    pandas_result['Location'] = pandas_result['Location'].fillna('None')  
    pandas_result = pandas_result.sort_values(by=['AverageAnswersCount', 'AccountId'], ascending=[False, True]).head(10)

    pandas_result.reset_index(drop=True, inplace=True)

    # Closing the database connection
    conn.close()

    # Before comparison, converting data types
    sql_result['AccountId'] = sql_result['AccountId'].astype(float)
    sql_result['AverageAnswersCount'] = sql_result['AverageAnswersCount'].astype(float)

    pandas_result['AccountId'] = pandas_result['AccountId'].astype(float)
    pandas_result['AverageAnswersCount'] = pandas_result['AverageAnswersCount'].astype(float)

    # Cleaning and standardizing text
    pandas_result['DisplayName'] = pandas_result['DisplayName'].str.strip().str.lower()
    sql_result['DisplayName'] = sql_result['DisplayName'].str.strip().str.lower()
    pandas_result['Location'] = pandas_result['Location'].str.strip().str.lower().replace('none', None)
    sql_result['Location'] = sql_result['Location'].str.strip().str.lower().replace('none', None)

    return sql_result, pandas_result
