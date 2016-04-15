
# Status: Experiment

This code is inspired by a paper on shredding.

> Cheney, James, Sam Lindley, and Philip Wadler. "Query shredding: efficient relational evaluation of queries over nested multisets." Proceedings of the 2014 ACM SIGMOD international conference on Management of data. ACM, 2014.
> 
> Abstract: Nested relational query languages have been explored extensively, and underlie industrial language-integrated query systems such as Microsoft's LINQ. However, relational databases do not natively support nested collections in query results. This can lead to major performance problems: if programmers write queries that yield nested results, then such systems typically either fail or generate a large number of queries. We present a new approach to query shredding, which converts a query returning nested data to a fixed number of SQL queries. Our approach, in contrast to prior work, handles multiset semantics, and generates an idiomatic SQL:1999 query directly from a normal form for nested queries. We provide a detailed description of our translation and present experiments showing that it offers comparable or better performance than a recent alternative approach on a range of examples.
>
> http://homepages.inf.ed.ac.uk/slindley/papers/shredding.pdf


# Aim

Translate nested queries expressed as om.next queries into regular SQL queries.

# Method

The core issue is that sql queries aren't great at nested data.  They return tables of information, not graphs. 

The approach taken is:

* Shred the nested query up into a set of regular sql queries
* Execute the sql queries
* Stitch the results into the nested structure

# Results

Shredding: work in progress but seems to be looking good.

Executing: no code

Stitching: no code



