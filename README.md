# Goda DB
*one case - one algorithm*

## Data types
### Data
On disc data storage.

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | set, get by id |
| Latency (random) | <1 ms with get 100 values, in any place of index, depend from HDD/SSD/Cache |
| Memory usage | 8MB per 1,000,000 values, 8 bytes/entry |
| Write (Data) | 80,000 r/s |
| Read (random) | 70,000 r/s |
| Read (segment) | 100,000 r/s |
| Use cases | storing data |

### Memo
Like Data type storage, but in-memory.

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | selecting Tree/Tags without Data field, custom dictionary for compression (30-60%) |
| Latency | <1 ms with get 200 values, in any place of index |
| Memory overhead | 35/60MB per 1,000,000 values, 35/60 bytes/entry |
| Write | 150,000 r/s |
| Read | 200,000 r/s |
| Use cases | storing meta/properties data, fastest selection |

### Hash
Key-Value in-memory index for disc Data storage.

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | set by key, get by key |
| Latency | <1 ms with get 100 values, in any place of index |
| Memory usage | 30MB per 1,000,000 values, 30 bytes/entry |
| Write (Data+Hash) | 70,000 r/s |
| Use cases | storing data, storing key for data, external id |

### Tags
Ordered by adding.

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | order, offset, limit, selection types: single [tag1], intersect [tag1,tag2], intersect by group [tag1,tag2,tag3+tag4], exclude [tag1,tag2,^tag3] |
| Latency | <1 ms with get 100 values, in any place of index |
| Memory usage | 4MB per 1,000,000 values, 4 bytes/entry |
| Write (Data+Hash+Tags) | 65,000 r/s |
| Use cases | tags, terms, taxonomy, navigation, pagination, counting, faceted classification |
```javascript
POST /index/tags
{
	"Tags": ["Tag 1", "Tag 2"],
	"Range": {
		"Order": "ASC", // Optional (ASC|DESC), default ASC
		"Offset": 0,    // Optional (0-4294967295), default 0
		"Limit": 10     // Optional (0-4294967295), default 0
	},
	"Memo": 0 // Optional (0|1), default 0
}
```

### Tree
Custom ordering by value (0-4294967295).

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | order, min/max-range, limit |
| Latency | <1 ms with get 100 values, in any place of index |
| Memory usage | 8MB per 1,000,000 values, 8 bytes/entry |
| Write (Data+Hash+Tags+Tree) | 55,000 r/s |
| Use cases | sorting data, price, quantity, counting by range |

### Full
Full-text search inverted index.

| | |
| ------------ | ------------- |
| Complexity | -> O(1) |
| Features | splitting text by words on Set, maximum word length 8 symbols |
| Latency | <1 ms with get 100 values, in any place of index |
| Memory usage | 6MB per 1,000,000 values, 6 bytes/entry |
| Write (Data+Hash+Tags+Tree+Full) | 35,000 r/s |
| Use cases | text search |

Summmary

| | |
| ------------ | ------------- |
| Memory usage | 10MB+ per 1,000,000 values, 10+ bytes/entry, instance - depends of requests |
```javascript
POST /index/set
{
	"Value":{
		"Data":'// Place your data in any format (string/bytes/etc)
			"Title":"Title field 0",
			"Text":"Text text text.",
			... // Add all your data what you want store, include index data like in Tree
		',
		"Hash": ["Key for this field"],
		"Tags": ["Tag 429", "Tag 963", "Tag 822"],
		"Full": ["Full-text search field 0"],
		"Tree": {
			"Data": 1480189402
		},
		"Options":{
			"Reserve": 64,     // Optional, default 0; Reserved space for this Data in bytes (Required for Update)
			"HashDuplicate": 0 // Optional, default 0; 0 - not insert on any duplicate key; 1 - not insert if first key duplicate; 2 - insert duplicate without hash key
		}	
	}
}
```

Goda DB have read priority (with identical concurrency performance will be X writes and 10*X reads). Better use separate servers for high reading and writing.

JSON requests:

| | |
| ------------ | ------------- |
| Read(100%) | 70,000 r/s |
| Read(70%)/Write(30%) | 30,000/10,000 r/s |
| Read(50%)/Write(50%) | 15,000/15,000 r/s |
| Read(30%)/Write(70%) | 9,000/18,000 r/s |
| Write(100%) | 30,000 r/s |

Performance notes:
Tested on typical desktop hardware. Sender requests and DB instance on only one machine.
In real world with DB on other machine results will be better (x2), not tested at that moment.

| | |
| ------------ | ------------- |
| CPU | Intel(R) Core(TM) i5-4670 CPU @ 3.40GHz, Cores/Logical: 4 |
| Mem | DDR3 1333 MHz |
| HDD | WDC WD30EFRX-68EUZN0 |

## Examples
```json
POST /index/tags
{
	"Tags": ["Tag 1", "Tag 2"],
	"Range": {
		"Offset": 0,
		"Limit": 10
	}
}

```

## FAQ

### Where I can use Goda DB?
Any type of web sites/apps where you no need difficult sorting by many values.
Most of data-types can be reworked for optimal performance/latency with this solution.

### Where using Goda DB not good idea?
OLAP (difficult indexes, joins and sorting), high-critically data (full ACID), high-concurrent write (sharding).

### Replication
Master/Slave async binary replication. Slaves read-only for strong consistency.

### What about perfomance, this is maximum optimization?
At that moment not all code optimized to maximum (Goda not contain C and ASM code), but performance realy good.

### Maximum database size?
Tested database up to 30,000,000 values (Data+Hash+Tags+Tree+Full), 50GB storage, 3GB memory. May not have optimizations for larger databases now.


### I care about my data, what about ACID and other acronyms?

ACID - Atomic (partial), Consistency (partial), Isolation (partial), Durability (yes).
Goda DB not provide transaction mechanism in this time. ACID about transactions.

CAP - Consistency (partial), Availability (yes), Partition tolerance (yes)

BASE - Basically Available (yes), Soft-state (yes), Eventually consistent (yes)

Data can be corrupted only if process die on write (data in memory buffer). Already writed data and database not be corrupted.


### Hash collision resolution?
If we doing collision resolution we get x2-x3 memory usage for storing hashes in memory and not stable write speed (x/3-x/2).
64-bit hashtable with FNV-1a hash give below 1 collision per 60,000,000 keys. 

### Golang GC not optimal work with large data?
GC problem with high latency on some little percentiles not resolved, but maybe will better on Go 1.8
Also solution maybe with off-heap memory, but this not implemented now.
https://github.com/golang/proposal/blob/master/design/17503-eliminate-rescan.md
https://talks.golang.org/2015/go-gc.pdf

### Words about algorithm
All algorithms contain mix of technologies/realizations.
Some words: Slice, Map, Index, Hashtable, Bitmap, Inverted Index, Tree.

### Influences
Redis, Elasticsearch/Lucene/Sphinx, PostgreSQL/SQLite, Berkeley DB.

### Features Goda DB by keywords
High-effectivity, Low memory usage, High-performance, Log-structured, Append-only, Off-memory storage, Index, Bitmap, Tree, Tags, Full-text search, Inverted Index, JSON API, BASE, CAP (AP), Replication.

### Memory storage, Optimization Memo field
*__ATTENTION!!!__ This part of features difficult.*
Memo, like Data type, but in-memory.
You can add here anything data associated with Data ID, this field fine for index with metadata (snippets) for fast future selections by Tags/Tree without touching disk.

- Something like this:
```
2016-12-10;Title title title;domain.com;Tag 1,Tag 2,Tag 53;http://cdn2.aefekef.example.com/3288/32se/35gf/eski/fjeu/img_1.jpg
```
- You can select type of overhead in config, EffectiveMemo variable (RESTART REQUIRED):
	- true for 35 bytes per each Data entry;
	- false for 60 bytes per entry with Memo field (DEFAULT).
- Create custom dictionary
	- *__ATTENTION!!!__ DO THIS OPERATION BEFORE ADDING ANY MEMO FIELDS TO DATABASE*
	- *IF YOU ALREADY HAVE MEMO FIELDS: 1. STOP SERVER; 2. CHANGE DICTIONARY; 3. START SERVER.*
	- Compression with custom dictionary can be 30-60% depends of data.
	- Compressed example (symbol ÿ = 0xFF byte, don't use it in your data): 
	- ```ÿ112-10;Title title tile;ÿ2;ÿ3,ÿ4,ÿ5;ÿ63288/32se/35gf/eski/fjeu/ÿ7```
- Manual creating dictionary
	1. 255 strokes maximum, 255 values maximum and 3+ length per entry for addding to dictionary.
	2. Modify file "data/indexname/.dictionary" (RESTART REQUIRED).
	3. One stroke - one entry. Be careful with spaces and other symbols, this data will used without modification.
- Performance with 100 elements dictionary.

	| | |
	| ------------ | ------------- |
	| Compress | 180,000 r/s |
	| Decompress | 280,000 r/s |
	| Compress+Decompress | 110,000 r/s |

### Tips and tricks
Add Goda service on CentOS 7
```bash
cd /usr/lib/systemd/system
cp /usr/lib/systemd/system/goda.service /usr/lib/systemd/system/goda.service
vim /usr/lib/systemd/system/goda.service
```

### What may be soon?
Thinking about: Raft, Master/Master, Sharding, Split-brain, Failover.

### Tasks
- [ ] Cache
- [x] Replication