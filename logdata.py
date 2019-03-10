#!/usr/bin/env python3
import psycopg2
# importing for Proper date format
from datetime import datetime


# TOP THREE ARTICLES OF ALL THE TIME?
result_1 = ("* TOP THREE ARTICLES OF ALL THE TIME?")
query_1 = (
    "SELECT title, count(*) as views FROM articles "
    "JOIN log "
    "ON articles.slug = substring(log.path, 10) "
    "GROUP BY title ORDER BY views DESC LIMIT 3;")

# TOP THREE ARTICLE AUTHORS OF ALL THE TIME?
result_2 = ("* TOP THREE ARTICLE AUTHORS OF ALL THE TIME?")
query_2 = (
    "SELECT authors.name, count(*) as views FROM articles INNER "
    "JOIN authors on articles.author = authors.id INNER JOIN log "
    "on log.path like concat('%', articles.slug, '%') WHERE "
    "log.status like '%200%' group "
    "by authors.name ORDER BY views DESC")

# On which days did more than 1% of requests lead to ERRORS
result_3 = ("* On which days did more than 1% of requests lead to ERRORS?")
query_3 = (
    "SELECT day, perc FROM ("
    "SELECT day, round((sum(requests)/(SELECT count(*) FROM log WHERE "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc FROM (SELECT substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests FROM log WHERE status like '%404%' group by day)"
    "as log_percentage group by day order by perc DESC) as final_query "
    "WHERE perc >= 1")


def connect(database_name="news"):
    """Connect or check database connection"""
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except psycopg2.OperationalError as e:
        print("Unable to connect to the database...")


def get_query_results(query):
    # Return results
    db, cursor = connect()
    cursor.execute(query)
    return cursor.fetchall()
    db.close()


def query_results(query_results):
    print(query_results[1])
    for index, results in enumerate(query_results[0]):
        print("\t" + str(index+1) + ", " + str(results[0]) +
              " :"+"- "+str(results[1])+" Views")


def error_results(query_results):
    print(query_results[1])
    for results in query_results[0]:
        d = results[0]
        date_obj = datetime.strptime(d, "%Y-%m-%d")
        formatted_date = datetime.strftime(date_obj, "%B %d, %Y")
        print("\t" + "On " + str(formatted_date) + " leads to " +
              str(results[1]) + "% ERRORS")


if __name__ == '__main__':
    # get results
    popular_articles = get_query_results(query_1), result_1
    popular_article_authors = get_query_results(query_2), result_2
    error_days = get_query_results(query_3), result_3

    # print results
    print("  Getting Data....\n")
    query_results(popular_articles)
    query_results(popular_article_authors)
    error_results(error_days)
