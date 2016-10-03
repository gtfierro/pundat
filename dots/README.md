# DOTs

This is a bunch of code and thoughts for how to do the DOT traversal for archiving

## URI Semantics

A process identified by VK `A` wants to access historical data (this could be timeseries or metadata or otherwise) for some URI `<uri>`.

The timestamps mentioned here all concern the *stored* timestamp, which is not necessarily the time at which data was published.
For example, we may store old data in an archiver that was "published" before BOSSWAVE even existed; rather than using the timestamp
of the time the data was uploaded, we would use the actual timestamps of the data.

### Access DOTs

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

### Archival DOTs

For an archival DOT for URI `<uri>` with start time `t1` and end time `t2`, we build a DOT on the URI
`archive/start/t1/end/t2/<uri>`, where `t1` and `t2` are unix nanosecond timestamps. The existance of this DOT
allows `A` to query the archiver for data published in the range [t1, t2] *in addition* to the range
granted by the access DOTs.

The first problem to solve is how to bootstrap these ranges of data: who has permission to access the entire
history of a stream, and how does the archiver verify that this is true?

We delegate this role to the *namespace authority* for the URI on which `<uri>` exists; this namespace authority
can grant DOTs on `archive/start/+/end/+/<uri>`; these DOTs have creation/expiry times, but these are used entirely
for determining the validity of the archival DOT and not used for data access like they are with access DOTs. Instead,
the parameters in the URI determine the valid range of archival access.

The namespace authority can delegate the permission giving to another key (which is likely to be the dominant use case
involving the namespace authority). This can take several forms:

* Grant on `archive/start/+/end/+/<uri>`: the recipient entity has permission to see all historical data
* Grant on `archive/start/t1/end/t2/<uri>`: the recipient entity has permission to see all historical data in the range [t1,t2],
  where t1 and t2 are Unix nanosecond timestamps
* Grant on `archive/start/+/end/t2/<uri>`: the recipient entity has permission to see all historical data up to t2 (the range [0, t2])
* Grant on `archive/start/t1/end/+/<uri>`: the recipient entity has permission to see all historical data after t1 (the range [t1, infinity])

This initial grant from the namespace authority is the "root" that validates all archival DOT chains. The `archive` namespace will be open so that
"everyone" has C (consume) access and grant access on the full namespace. Anyone can grant "consume" DOTs on the archive namespace, but when the
archiver receives a request for archival data on `<uri>`, it will pull in the full family of DOT chains on that URI for the requesting entity,
and ensure that it only considers valid chains that terminate on a DOT from the namespace authority.

So how do we determine the valid archival ranges?  This is essentially the same approach as we use for access DOTs, but pulling our start/end times
from the URI instead of from the creation/expiry times of the DOT. For each DOT chain, we take the intersection of the start/end times, and then
determine the archival ranges to be the union of these intersections.

For example (all grants are for `C` permission, not `C*` or `P` or `L`, and all numbers are timestamps)

* ns authority grants on `archive/start/1/end/50/<uri>` to entity `B`
* `B` grants on `archive/start/40/end/50/<uri>` to entity `A`

* ns authority grants on `archive/start/1/end/50/<uri>` to entity `C`
* `C` grants on `archive/start/20/end/25/<uri>` to entity `A`

* `D` grants on `archive/start/10/end/20/<uri>` to entity `A`

* ns authority grants on `archive/start/1/end/20/<uri>` to entity `E`
* `E` grants on `archive/start/30/end/40/<uri>` to entity `A`

Here, there are 4 archival DOTs on `<uri>` for entity `A`, but only some of them are valid.

For the first chain from `ns -> B -> A`, we get a valid chain that grants `A` archival access to data in the range [40, 50].

For the second chain from `ns -> C -> A`, we get a valid chain that grants `A` archival access to data in the range [20, 25], so
`A`'s current archival access span is [20,25] and [40,50].

The third chain does not terminate at the namespace authority, so it is not considered by the archiver.

The fourth chain *does* terminate at the namespace authority, but `E` only has archival access to data in the range [1,20], which is disjoint
from the range that `E` attempts to grant to `A`, which is [30,40]; thus, this DOT chain is not considered by the archiver and does not
permit `A` to access any further ranges of data.
