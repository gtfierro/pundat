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
[5, 15).
