#!/usr/bin/env python

import psycopg2


class LogAnalyzer(object):

    def __init__(self, dbname="news"):
        self.conn = psycopg2.connect("dbname=news")
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def most_popular_articles(self):
        '''Return articles sorted by popularity.
        Returns:
        - List of (article title, count) tuples.
        '''
        query = '''
        SELECT articles.title, COUNT(*)
        FROM articles, log
        WHERE CONCAT('/article/', articles.slug) = log.path
        GROUP BY articles.title
        ORDER BY COUNT(*) DESC;
        '''
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def most_popular_authors(self):
        '''Return authors sorted by popularity.
        Returns:
        - List of (author, count) tuples.
        '''
        self.cursor = self.conn.cursor()
        query = '''
        SELECT authors.name, COUNT(*)
        FROM articles, authors, log
        WHERE CONCAT('/article/', articles.slug) = log.path
        AND articles.author = authors.id
        GROUP BY authors.name
        ORDER BY COUNT(*) DESC;
        '''
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def error_days(self):
        '''Return days with more than 1% requests errant, sorted by percentage.
        Returns:
        - List of (date, percentage) tuples.
        '''
        self.cursor = self.conn.cursor()
        query = '''
        CREATE VIEW log_days AS
        SELECT LEFT(TEXT(log.time), STRPOS(TEXT(log.time), ' ') - 1) as day,
               (CASE WHEN status LIKE '4%' OR status LIKE '5%'
                THEN 100.0 ELSE 0.0 END) as error,
               status
        FROM log;

        SELECT log_days.day, AVG(error) as percentage
        FROM log_days
        GROUP BY log_days.day
        HAVING AVG(error) > 1.0
        ORDER BY percentage DESC;
        '''
        self.cursor.execute(query)
        return self.cursor.fetchall()


if __name__ == '__main__':

    analyzer = LogAnalyzer()
    print("1. What are the most popular three articles of all time?")
    r = analyzer.most_popular_articles()
    print("\n".join(["\t%s %d" % v for v in r]))

    print("2. Who are the most popular article authors of all time?")
    r = analyzer.most_popular_authors()
    print("\n".join(["\t%s %d" % v for v in r]))

    print("3. On which days did more than 1% of requests lead to errors?")
    r = analyzer.error_days()
    print("\n".join(["\t%s %.3f" % v for v in r]))
    analyzer.close()
