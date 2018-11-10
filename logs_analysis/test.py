#!/usr/bin/env python

from analyze_logs import LogAnalyzer

analyzer = LogAnalyzer()

# 1. What are the most popular three articles of all time?
r = analyzer.most_popular_articles()
assert(r[0][1] == 338647)

# 2. Who are the most popular article authors of all time?
r = analyzer.most_popular_authors()
assert(r[0][1] == 507594)

# 3. On which days did more than 1% of requests lead to errors?
r = analyzer.error_days()
assert(r[0][0] == "2016-07-17")

print("Tests passed.")
