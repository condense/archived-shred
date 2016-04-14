
Status: Experiment

This code is inspired by a paper on shredding.

> Query Shredding: Efficient Relational Evaluation of Queries ...
> homepages.inf.ed.ac.uk/slindley/papers/shredding.pdf
> by J Cheney - ‎Cited by 7 - ‎Related articles
> Query Shredding: Efficient Relational Evaluation of. Queries over Nested Multisets. James Cheney. University of Edinburgh jcheney@inf.ed.ac.uk. Sam Lindley.


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



