# DOTs

This is a bunch of code and thoughts for how to do the DOT traversal for archiving

## URI Semantics

A process identified by VK `A` wants to access historical data (this could be timeseries or metadata or otherwise) for some URI `<uri>`.

Some historical access is granted by the existance of DOTs granted to `A` on that URI.
For all *valid* DOT chains, the permitted access ranges to the historical data of the stream
are the union of the intersections for valid times for each DOT chain.
These semantics require `C` or `C*` (consume) permissions; publish (`P`) permissions do not
guarantee that the key `A` was allowed to see the data published (multiple publishers may have
had access, with the intention of keeping them private from each other).

For example, using logical timestamps, we may have 2 DOT chains for key `A` granted on `<uri>`:

* ns authority grants permission `C` on `<uri>` to entity `B`. Granted: 1, Expire: 20
* `B` grants permission `C` on `<uri>` to entity `C`. Granted: 10, Expire: 20
* `C` grant permission `C` on `<uri>` to entity `A`. Granted: 10, Expire: 15

* ns authority grants permission `C` on `<uri>` to entity `D`. Granted: 1, Expire: 20
* `D` grants permission `C` on `<uri>` to entity `A`. Granted: 5, Expire: 10

In this case, entity `A` has 2 DOTs on `<uri>`, one from time 5 to 10, and the other from time
10 to 15 (these two ranges are the intersections of the grant times on the DOT chains). We
take the union of these two DOTs to determine that `A` has access to data published in the range
[5, 15].

If this entity `A` wishes to interact w/ an archival service to retrieve data that was published on
`<uri>` within this range, the archiver does the work of assembling the DOTs and merging/intersecting
them to determine that no matter what range of data is requested, this range should be masked by [5,15].

These DOTs, which are granted on the actual URI itself, are called *access DOTs*.

This is all well and good, but what if we want to grant entity `A` the ability to query "historic" data
that was published outside of one of the "valid" ranges? For this, we need *archival DOTs*

For an archival DOT for URI `<uri>` with start time `t1` and end time `t2`, we built a DOT on the URI
`archive/t1/t2/<uri>`, where `t1` and `t2` are unix nanosecond timestamps.
