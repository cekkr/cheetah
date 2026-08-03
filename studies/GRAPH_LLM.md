# GRAPH_LLM.md — teaching a small model to learn in real time with Cheetah

A study of the loop between a language model and `cheetah-server`: how an LLM **teaches** the database
what it just heard, and how it **learns** — recalls, corrects, and improves its own routing — from what
is already stored. The target is a model that *knows little by design* and stays useful because
knowledge lives in the database, arrives during the conversation, and is queryable a millisecond later.

Scope note: unlike the rest of [`studies/`](.), this file is **not aspirational**. Every transcript
below was captured from a build of the checked-out revision (CLI and TCP), and the payload lines are
the real base64 decoded. Where something is a design sketch rather than a verified behaviour, it says
so explicitly.

Companion reading:

- [`README.md` → Sentences → Graph → Answers](../README.md#sentences--graph--answers-llm-recipes) —
  the sentence↔command mapping, uncertainty encoding, and the command grammar. **This document assumes
  it** and does not repeat the mapping table.
- [`AGENTS.md`](../AGENTS.md) — engine contracts. `ExecuteCommand` is the authority on syntax.
- [`CONCEPTS.md`](../CONCEPTS.md) — the original context-relativism intent behind prediction tables.

---

## 1. The premise: competence in the model, knowledge in the database

Split the two things a chat system is usually asked to do at once:

| Concern | Owner | Why |
| --- | --- | --- |
| Language: parsing, phrasing, inference over given facts | the model | what small models are still good at |
| Knowledge: what is true, of whom, since when, how sure | Cheetah | writable at conversation speed, inspectable, correctable |
| Recall policy: what to fetch, how much, when to give up | the adapter | a budget question, not a language question |

Three consequences drive every pattern in this document:

1. **The model never answers a factual question from its parameters.** It answers from returned rows,
   or it says it does not know. A model that guesses cannot be corrected — the wrong answer leaves no
   trace in the database.
2. **Absence is information.** `matches=0` and `ERROR,node_not_found` mean *nothing is recorded*, which
   is a legitimate, useful answer — and the trigger to *ask* and then *write*.
3. **Writing is part of answering.** Every turn that resolves something ends with a write, so the same
   question is cheaper next time. A read-only memory never learns.

---

## 2. Three memory tiers, three parts of the engine

Cheetah offers three storage shapes; a learning agent needs all three, for different lifetimes.

| Tier | What it holds | Commands | Lifetime |
| --- | --- | --- | --- |
| **Episodic** — verbatim | the exact utterance, unparsed | `INSERT` + `PAIR_SET episode:…` | append-only, prunable per prefix |
| **Semantic** — facts | entities, relations, confidence | `GRAPH_*` | upserted and corrected forever |
| **Procedural** — habits | which intent a phrasing means, ranked candidates | `PREDICT_*` | trained online, decays by retraining |

Working memory (the current turn) stays in the prompt and is never written until it is worth keeping.

### 2.1 Episodic: keep the sentence, not just your reading of it

Extraction is lossy and your extractor will improve. Store the raw text first, so a later pass can
re-read it with better prompts:

```text
> INSERT:72 My cat is a female siamese, very cute and sweet. But she may be sterile.
SUCCESS,key=1
> PAIR_SET episode:20260722T1701Z/001 1
SUCCESS,pair_set
> PAIR_GET episode:20260722T1701Z/001
SUCCESS,key=1
> READ 1
SUCCESS,size=72,value=My cat is a female siamese, very cute and sweet. But she may be sterile.
```

The pair key is `episode:<timestamp>/<seq>`, so the trie stores episodes in **chronological order** for
free: `PAIR_SCAN episode:2026 …` is "everything from 2026", and cursors page through a day without
loading it.

```text
> PAIR_SCAN episode: 8
SUCCESS,count=6,items=657069736f64653a323032363037323254313730315a2f303031:1;657069736f64653a323032363037323254313730335a2f303032:26;657069736f64653a323032363037323254313734305a2f303033:42;657069736f64653a323032363037323254313830325a2f303034:46;657069736f64653a323032363037323254313831355a2f303035:59;657069736f64653a323032363037323254313831375a2f303036:68
```

Replaying episodes does **not** need one `READ` per key — the counts reducer streams the stored bytes
inline as base64:

```text
> PAIR_REDUCE counts episode: 2
SUCCESS,reducer=counts,count=2,next_cursor=x657069736f64653a323032363037323254313730335a2f303032,items=657069736f64653a323032363037323254313730315a2f303031:1:TXkgY2F0IGlzIGEgZmVtYWxlIHNpYW1lc2UsIHZlcnkgY3V0ZSBhbmQgc3dlZXQuIEJ1dCBzaGUgbWF5IGJlIHN0ZXJpbGUu;657069736f64653a323032363037323254313730335a2f303032:26:SSB3b3VsZCBsaWtlIHRvIGhhdmUgYSBsaXR0ZXIgZm9yIG15IGNhdC4=
# item 1 payload decodes to: "My cat is a female siamese, very cute and sweet. But she may be sterile."
```

That is the whole consolidation input: one command, one page, `next_cursor` for the rest.

Frequently recalled wording can also travel with the semantic node that uses it. Store complete
sentences as bounded node references (base64 because the protocol splits on spaces), then ask recall
to hydrate them. An evidence edge whose `props.src` is an `INSERT` key contributes that verbatim
episode too:

```text
> GRAPH_NODE_SET id=condition:sterile references=<base64 of [{"id":"sterile-claim","text":"The cat may be sterile.","source":"owner","ordinal":1}]>
SUCCESS,node_set,id=condition:sterile
> GRAPH_RECALL seeds=cat:luna references=1 reference_limit=8
SUCCESS,command=GRAPH_RECALL,…,references=2,…,payload=<base64>
# the condition association carries the stored sentence and, when its evidence path cites props.src,
# the original episode payload. The adapter can ground an answer in complete wording, not token ids.
```

`references` is not a second fact store: graph edges remain authoritative for meaning and
confidence. It is the readable provenance attached to a node. Omission preserves the current list,
`references=-` clears it, and reference words enter the derived term index so a free-text seed can
land on remembered language.

### 2.2 Semantic: the graph, covered in the README

See [Sentences → Graph → Answers](../README.md#sentences--graph--answers-llm-recipes). The only thing
worth repeating here is the shape of a *good* fact: an edge whose type is a stable verb, whose
`confidence` says how sure it is (§7), and whose props carry `src` — the episodic key it came from.

### 2.3 Procedural: prediction tables, section 8

---

## 3. Identity discipline: ids are the contract

Everything hinges on the model producing the **same id** for the same thing across turns, sessions and
extractors. The engine will not help — ids are opaque bytes to it.

- **Shape:** `<type>:<slug>` — `person:marco`, `cat:luna`, `trait:sweet`, `intent:breed_litter`,
  `hypothesis:catsitter`, `episode:<ts>/<seq>`. The `type:` prefix is what makes
  `PAIR_SCAN`/`PAIR_SUMMARY` able to inventory one kind of thing.
- **Slugging is the model's job:** lowercase, `_` for spaces, no accents. `id=` is parsed as a
  whitespace-separated token, so a space silently truncates the id (§11).
- **Resolve before you mint.** "Marco" in turn 7 must reach `person:marco` from turn 2. Keep an alias
  edge (`person:marco -[:alias]-> alias:marco_from_acme`) or, for the current conversation, a small
  in-prompt table of ids already used.
- **Payload keys are global and are not yours to guess.** `INSERT` returns a key from a single
  monotonic counter shared with graph records, so consecutive utterances are **not** consecutive keys:

  ```text
  > INSERT:72 My cat is a female siamese, …      → SUCCESS,key=1
  … 20 graph writes in between …
  > INSERT:41 I would like to have a litter for my cat.   → SUCCESS,key=26
  > INSERT:46 The vet checked her: she is fertile after all. → SUCCESS,key=42
  ```

  An adapter must capture `key=` from each `INSERT` response and use *that* in `PAIR_SET` and in
  `props.src`. A counter maintained client-side will point at unrelated graph payloads.

---

## 4. The teach loop (write path)

Five steps per user turn that contains a statement. Steps 1 and 5 are the ones usually skipped, and
skipping them is what makes a memory rot.

**Step 1 — log verbatim.** `INSERT` + `PAIR_SET episode:…` (§2.1). Cheap, and it is the only copy that
survives a bad extraction.

**Step 2 — probe before writing.** Ask what is already known about the entities you are about to touch;
this is one command and it prevents duplicate ids and contradictory writes:

```text
> GRAPH_NEIGHBOR_TYPES id=cat:luna direction=out limit=16 weighted=1
SUCCESS,count=3,next_cursor=*,payload=<base64>
# decodes to: [{"type":"has_breed","count":1,"weighted":1},{"type":"has_condition","count":1,"weighted":1},{"type":"has_trait","count":1,"weighted":1}]
```

**Step 3 — write facts, batched.** One round trip for the whole extraction, with
`continue_on_error=1` so a single malformed row does not lose the turn:

```text
> GRAPH_EDGE_SET_BATCH items=<base64 of [{"from":"cat:luna","to":"trait:cute","type":"has_trait","weight":0.9}, {"from":"cat:luna","to":"trait:sweet","type":"has_trait","weight":0.9}, {"from":"cat:luna","to":"condition:sterile","type":"has_condition","confidence":"possible"}]> continue_on_error=1
SUCCESS,requested=3,applied=3,created=3,updated=0,failed=0
```

`created` vs `updated` is a free signal of how much of the turn was actually new. Endpoint nodes are
auto-created as stubs; pass `autocreate=0` if you want a write to fail rather than invent a node.

**Step 4 — attach provenance.** `props.src` is the episodic key, so every derived fact walks back to
the sentence that produced it:

```text
> GRAPH_EDGE_GET from=cat:luna to=condition:sterile type=has_condition
SUCCESS,id=…,payload=<base64>
# decodes to: {"from":"cat:luna","to":"condition:sterile","type":"has_condition","weight":1,
#              "confidence":0,"modality":"ruled_out","props":{"source":"vet","src":"40"},…}
> READ 40
SUCCESS,size=46,value=The vet checked her: she is fertile after all.
```

This is what makes an answer auditable — *"I believe she is fertile because on 22 July you told me the
vet checked her"* — and what lets a later pass re-extract from the original wording.

**Step 5 — update the statistics.** If the turn revealed which intent a phrasing carries, train the
router (§8). One `PREDICT_TRAIN` per confirmed turn is enough.

### 4.1 The upsert trap: partial updates are not partial

Both `GRAPH_NODE_SET` and `GRAPH_EDGE_SET` **replace** the fields you pass and **default** the ones you
omit. Two verified consequences that silently corrupt a memory:

```text
> GRAPH_NODE_SET id=cat:luna labels=animal,cat props={"name":"Luna","sex":"female"}
> GRAPH_NODE_SET id=cat:luna props={"age":3}
> GRAPH_NODE_GET id=cat:luna
# decodes to: {"id":"cat:luna","labels":["animal","cat"],"props":{"age":3},…}
#             name and sex are gone — props was replaced, not merged
```

```text
> GRAPH_EDGE_SET from=a to=b type=likes weight=0.4 props={"seen":1}
> GRAPH_EDGE_SET from=a to=b type=likes props={"seen":2}
> GRAPH_EDGE_GET from=a to=b type=likes
# decodes to: {…,"weight":1,"props":{"seen":2},…}
#             weight silently jumped 0.4 → 1.0, because omitted weight defaults to 1.0
```

**Rule: read the record, merge in the model, write the complete record.** Omitting a field is not a
way to keep it, with two deliberate exceptions:

- `labels`/`props` on a node — omitting the *whole* argument preserves the stored value;
- `confidence`/`modality`/`ambiguity` on an edge (§7) — a belief is preserved unless restated, because
  a write that meant to touch a prop must not be able to promote a hedge to a certainty. Clear one on
  purpose with `confidence=-`.

`weight` has no such protection: it is traversal strength, and it resets.

---

## 5. The recall loop (read path)

Answering is a **budget** problem: each command is a round trip and a chunk of context window. Climb
the ladder only as far as the question requires, and stop at the first rung that answers it.

| Rung | Command | Cost | Use when |
| --- | --- | --- | --- |
| 0 | `GRAPH_NODE_GET id=X` | one record | the question is about X's own attributes |
| 1 | `GRAPH_NEIGHBOR_TYPES id=X` | histogram, no edge hydration | "what do I know about X?" — decide where to spend the next call |
| 2 | `GRAPH_DEGREE id=X` | one number | "do I know X well?" / ranking several candidates |
| 3 | `GRAPH_NEIGHBORS id=X type=R` | one adjacency page | a known relation, one hop, either direction |
| 4 | `GRAPH_QUERY … WHERE …` | index-served filter | a condition on weight or props |
| 5 | `GRAPH_QUERY … HOPS 1..n` | bounded traversal | chains: reporting lines, provenance, containment |
| 6 | `GRAPH_RECALL seeds=X,Y,…` | multi-seed spread, budget-bounded | the question names no relation, or you want what you did *not* ask for |

Rungs 1 and 2 exist so the model can decide *not* to pay for 3–5. A histogram is a few dozen tokens;
hydrating a fan-out is hundreds.

Answer template that keeps a small model honest — three slots, each filled from a row:

```
<claim>, because <edge type + endpoint>, recorded <weight/modality>. [Source: <src episode text>]
Unknown: <what returned matches=0>.
```

### 5.1 When you don't know what to ask

Rungs 0–5 all need the question already shaped: an id, usually a relation. A conversation rarely
arrives that way — it touches three or four things at once and the useful move is to see what they
have in common. That is rung 6.

```text
# the graph: luna and marco both live in Berlin, and share nothing else
GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed
GRAPH_EDGE_SET from=breed:siamese to=trait:vocal type=has_trait
GRAPH_EDGE_SET from=cat:luna to=city:berlin type=lives_in
GRAPH_EDGE_SET from=person:marco to=city:berlin type=lives_in
GRAPH_EDGE_SET from=person:marco to=hobby:sailing type=likes
GRAPH_EDGE_SET from=city:berlin to=country:germany type=located_in
GRAPH_EDGE_SET from=city:berlin to=city:berlino type=alias

[cheetah_data/default]> GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 limit=8
SUCCESS,command=GRAPH_RECALL,seeds=2,resolved=2,visited=8,expanded=6,hydrated=15,count=6,bridges=3,truncated=0,precision=0.100,decay=0.55,cache_decay=1,decay_relations=0,decay_profile=-,payload=<base64>
# associations decode to, in order:
#  city:berlin      score 0.7975   novelty 0.39875  distance 1  sources 2  ← both seeds, one hop
#  city:berlino     score 0.771994 novelty 0.385997 distance 1  sources 2  ← an alias: a hop, no distance
#  breed:siamese    score 0.55     novelty 0.1375   distance 1  sources 1
#  hobby:sailing    score 0.55     novelty 0.1375   distance 1  sources 1
#  country:germany  score 0.513494 novelty 0.342329 distance 2  sources 2  ← both seeds, two hops
#  trait:vocal      score 0.3025   novelty 0.100833 distance 2  sources 1
```

Read the columns, not just the order:

- **`score`** ranks by "how strongly is this lit up" — `city:berlin` beats every direct neighbour
  because two seeds reach it and their activations combine (`1 − Π(1 − aᵢ)`), not because it is
  closer.
- **`novelty`** ranks by "how much of this did I not already know": `country:germany` (0.342329) over
  `breed:siamese` (0.1375), even though `breed:siamese` scores higher. A direct neighbour of one seed
  is the answer to a question you could have asked; a two-hop node both seeds reach is not. Note
  `city:berlino` stays at distance 1: crossing an alias costs a hop but no distance, because an alias
  is not a different subject.
- **`via`** is the justification — the actual edges, each with its `weight`, `confidence` and
  `modality`. Quote it. An association without its path is a hallucination waiting to happen.
- **`sources`** says *which* seeds lit it and how far each one is. One source means "this belongs to
  one of your topics"; two means "this is where your topics meet".

Two habits worth keeping:

```text
# 1. the convergence question — "what do these have to do with each other?"
[cheetah_data/default]> GRAPH_RECALL seeds=cat:luna,person:marco hops=3 precision=0.05 min_sources=2
SUCCESS,…,count=5,bridges=5,…
# only nodes more than one seed reaches; everything single-topic disappears

# 2. seeds do not have to be ids — free text resolves through the lexical index and alias edges
[cheetah_data/default]> GRAPH_RECALL seeds=berlin hops=1 precision=0.1
SUCCESS,command=GRAPH_RECALL,seeds=1,resolved=2,…
# "seeds":[{"term":"berlin","matches":[{"id":"city:berlin","score":0.495,"match":"lexical"},
#                                      {"id":"city:berlino","score":0.47025,"match":"synonym"}]}]
```

The knob that matters is `precision`. It is a belief threshold as much as a distance one, because
activation is multiplied by each edge's confidence on the way: an edge recorded as `possible` passes
half of what a plain one passes, so raising `precision` drops hearsay before it drops distant facts.
It takes the same words as `edge.confidence` — `precision=probable` is 0.75.

`decay=` is the caller's base rather than the final per-edge rate. After a query shape has 32 cache
lookups, its smoothed hit rate contributes a quantized 0.75–1.25 factor; `cache=off` makes that part
neutral. A second, independent factor can be learned per relation through the existing prediction
engine:

```text
PREDICT_SET table=graph_recall_decay key=has_breed value=carry prob=0.9
PREDICT_SET table=graph_recall_decay key=has_breed value=stop prob=0.1
PREDICT_SET table=graph_recall_decay key=mentioned_near value=carry prob=0.1
PREDICT_SET table=graph_recall_decay key=mentioned_near value=stop prob=0.9
```

Recall maps the `carry`/`stop` scores to a bounded 0.5–1.5 relation factor. The table and each pair
are optional: missing or half-written evidence is exactly neutral, and recall never creates the
table just by reading it. The response exposes `decay`, `cache_decay`, `decay_relations`, and a
stable `decay_profile` digest. Synonym edges retain their fixed identity-preserving decay.

Recall never says "nothing". It says `count=0`, or it says `truncated=1` when the budget ran out
before the graph did — a partial answer that declares itself. Both are reportable; neither is a
negative fact about the world.

---

## 6. A seven-turn session, end to end

One conversation, captured over TCP against a fresh database. Non-essential responses elided; every
line shown is real. Watch the database change what the model can say.

### Turn 1 — teach

> *"My cat is a female siamese, very cute and sweet. But she may be sterile."*

```text
> INSERT:72 My cat is a female siamese, very cute and sweet. But she may be sterile.
SUCCESS,key=1
> PAIR_SET episode:20260722T1701Z/001 1
SUCCESS,pair_set
> GRAPH_NODE_SET id=person:owner labels=person
SUCCESS,node_set,id=person:owner
> GRAPH_NODE_SET id=cat:luna labels=animal,cat props={"name":"Luna","sex":"female"}
SUCCESS,node_set,id=cat:luna
> GRAPH_EDGE_SET from=person:owner to=cat:luna type=owns props={"src":"1"}
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8b3duc3xjYXQ6bHVuYQ
> GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed props={"src":"1"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfYnJlZWR8YnJlZWQ6c2lhbWVzZQ
> GRAPH_EDGE_SET from=cat:luna to=trait:sweet type=has_trait props={"src":"1","intensity":"very"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfdHJhaXR8dHJhaXQ6c3dlZXQ
> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=possible props={"src":"1"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
```

Three facts are asserted flatly and one is hedged — *"may be"* becomes `confidence=possible`, not a
different edge type and not a lowered `weight`. Every edge carries `src`, the episodic key.

### Turn 2 — ask, and hit a blocker

> *"I would like to have a litter for my cat."*

```text
> INSERT:41 I would like to have a litter for my cat.
SUCCESS,key=24
> PAIR_SET episode:20260722T1703Z/002 24
SUCCESS,pair_set
> GRAPH_NODE_SET id=intent:breed_litter labels=intent props={"goal":"litter","status":"open"}
SUCCESS,node_set,id=intent:breed_litter
> GRAPH_EDGE_SET from=person:owner to=intent:breed_litter type=wants props={"src":"24"}
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8d2FudHN8aW50ZW50OmJyZWVkX2xpdHRlcg
> GRAPH_EDGE_SET from=intent:breed_litter to=cat:luna type=about
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfGFib3V0fGNhdDpsdW5h

> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.modality >= 'possible' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"cat:luna","type":"has_condition","to":"condition:sterile","weight":1}]

> GRAPH_EDGE_SET from=intent:breed_litter to=condition:sterile type=blocked_by confidence=possible
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfGJsb2NrZWRfYnl8Y29uZGl0aW9uOnN0ZXJpbGU
> GRAPH_EDGE_SET from=intent:breed_litter to=action:vet_fertility_check type=requires
SUCCESS,edge_set,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfHJlcXVpcmVzfGFjdGlvbjp2ZXRfZmVydGlsaXR5X2NoZWNr
```

*"Luna is a female siamese, so a litter is possible — but there is a **possible** sterility on record.
Worth a vet fertility check first."* The plan is now **in the graph**, not only in the reply, and the
blocker inherits the same hedge as the fact behind it.

### Turn 3 — correction

> *"The vet checked her: she is fertile after all."*

```text
> INSERT:46 The vet checked her: she is fertile after all.
SUCCESS,key=40
> PAIR_SET episode:20260722T1740Z/003 40
SUCCESS,pair_set
> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=ruled_out props={"src":"40","source":"vet"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
> GRAPH_EDGE_DEL from=intent:breed_litter to=condition:sterile type=blocked_by
SUCCESS,edge_deleted,id=MXxpbnRlbnQ6YnJlZWRfbGl0dGVyfGJsb2NrZWRfYnl8Y29uZGl0aW9uOnN0ZXJpbGU
> GRAPH_NODE_SET id=intent:breed_litter labels=intent props={"goal":"litter","status":"unblocked"}
SUCCESS,node_set,id=intent:breed_litter

> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.modality >= 'possible' RETURN count
SUCCESS,return=count,matches=0,next_cursor=*

> GRAPH_QUERY MATCH (id='cat:luna')-[:has_condition]->(*) WHERE edge.modality = 'ruled_out' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"cat:luna","type":"has_condition","to":"condition:sterile","weight":1}]
```

Three properties of a correction done right: the belief is **demoted, not deleted** (`ruled_out`, with
`source:"vet"` recording who settled it, so *"weren't you saying she was sterile?"* is still
answerable); the **derived** edge that depended on it *is* deleted; and the same filter that used to
return the blocker now returns nothing, while an explicit `= 'ruled_out'` query still finds it.

### Turn 4 — a second domain, same mechanics

> *"My friend Sara moved to Lisbon and she is strictly gluten free."*

```text
> INSERT:63 My friend Sara moved to Lisbon and she is strictly gluten free.
SUCCESS,key=43
> PAIR_SET episode:20260722T1802Z/004 43
SUCCESS,pair_set
> GRAPH_NODE_SET id=person:sara labels=friend,person
SUCCESS,node_set,id=person:sara
> GRAPH_EDGE_SET from=person:sara to=city:lisbon type=lives_in props={"src":"43"}
SUCCESS,edge_set,id=MXxwZXJzb246c2FyYXxsaXZlc19pbnxjaXR5Omxpc2Jvbg
> GRAPH_EDGE_SET from=person:sara to=diet:gluten_free type=follows_diet props={"src":"43","strict":true}
SUCCESS,edge_set,id=MXxwZXJzb246c2FyYXxmb2xsb3dzX2RpZXR8ZGlldDpnbHV0ZW5fZnJlZQ
```

### Turn 5 — a question the graph cannot answer

> *"Who could look after Luna if I travel to visit Sara?"*

```text
> INSERT:52 Who could look after Luna if I travel to visit Sara?
SUCCESS,key=56
> PAIR_SET episode:20260722T1815Z/005 56
SUCCESS,pair_set

> GRAPH_NEIGHBORS id=cat:luna direction=in type=catsits limit=8
SUCCESS,count=0,next_cursor=*,payload=W10=
# decodes to: []

> GRAPH_NODE_SET id=hypothesis:catsitter labels=hypothesis props={"question":"who_catsits_luna","status":"open"}
SUCCESS,node_set,id=hypothesis:catsitter
> GRAPH_EDGE_SET from=person:owner to=hypothesis:catsitter type=unsure_about props={"src":"56"}
SUCCESS,edge_set,id=MXxwZXJzb246b3duZXJ8dW5zdXJlX2Fib3V0fGh5cG90aGVzaXM6Y2F0c2l0dGVy
> GRAPH_EDGE_SET from=hypothesis:catsitter to=cat:luna type=about
SUCCESS,edge_set,id=MXxoeXBvdGhlc2lzOmNhdHNpdHRlcnxhYm91dHxjYXQ6bHVuYQ
```

`count=0` is the answer: *"I have nobody on record who looks after Luna."* The model must not fill the
gap from its parameters — it records the open question instead, so the system can close it later.

### Turn 6 — an answer that is itself ambiguous

> *"Maybe Marco can catsit, or his brother Luca — I think Marco did it last summer."*

Two readings, one of them favoured. Both go in, as a group:

```text
> INSERT:79 Maybe Marco can catsit, or his brother Luca - I think Marco did it last summer.
SUCCESS,key=65
> PAIR_SET episode:20260722T1817Z/006 65
SUCCESS,pair_set

> GRAPH_AMBIGUITY_SET from=hypothesis:catsitter type=candidate group=who_catsits options=person:marco=0.7,person:luca
SUCCESS,ambiguity_set,group=who_catsits,options=2,confidence_sum=1.0000

> GRAPH_AMBIGUITY_GET from=hypothesis:catsitter group=who_catsits
SUCCESS,group=who_catsits,count=2,confidence_sum=1.0000,top=person:marco,top_modality=probable,payload=<base64>
# decodes to: [{"from":"hypothesis:catsitter","to":"person:marco","type":"candidate","confidence":0.7,"modality":"probable","ambiguity":"who_catsits",…},
#              {"from":"hypothesis:catsitter","to":"person:luca","type":"candidate","confidence":0.3,"modality":"unlikely","ambiguity":"who_catsits",…}]
```

The declared 0.7 leaves 0.3 for the alternative, and each number gets its word. The assistant can now
answer *"probably Marco, possibly his brother Luca"* — and `top`/`top_modality` give it that ranking
without decoding anything.

### Turn 7 — confirmation collapses the group

> *"Confirmed, it was Marco."*

```text
> INSERT:24 Confirmed, it was Marco.
SUCCESS,key=74
> PAIR_SET episode:20260722T1822Z/007 74
SUCCESS,pair_set

> GRAPH_AMBIGUITY_RESOLVE from=hypothesis:catsitter group=who_catsits winner=person:marco
SUCCESS,ambiguity_resolved,group=who_catsits,winner=person:marco,ruled_out=1,dropped=0
> GRAPH_EDGE_SET from=person:marco to=cat:luna type=catsits props={"src":"74","last_time":"summer_2025"}
SUCCESS,edge_set,id=MXxwZXJzb246bWFyY298Y2F0c2l0c3xjYXQ6bHVuYQ
> GRAPH_NODE_SET id=hypothesis:catsitter labels=hypothesis props={"question":"who_catsits_luna","status":"resolved"}
SUCCESS,node_set,id=hypothesis:catsitter
> GRAPH_EDGE_DEL from=person:owner to=hypothesis:catsitter type=unsure_about
SUCCESS,edge_deleted,id=MXxwZXJzb246b3duZXJ8dW5zdXJlX2Fib3V0fGh5cG90aGVzaXM6Y2F0c2l0dGVy

> GRAPH_QUERY MATCH (id='cat:luna')<-[:catsits]-(*) RETURN edges LIMIT 8
SUCCESS,return=edges,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","to":"cat:luna","type":"catsits","weight":1,"props":{"last_time":"summer_2025","src":"74"},…}]

> GRAPH_QUERY MATCH (id='hypothesis:catsitter')-[:candidate]->(*) WHERE edge.modality = 'ruled_out' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"hypothesis:catsitter","type":"candidate","to":"person:luca","weight":1}]

> GRAPH_NEIGHBORS id=person:owner direction=out type=unsure_about limit=8
SUCCESS,count=0,next_cursor=*,payload=W10=
```

The query that returned nothing in turn 5 now answers; the discarded reading survives as *excluded*
rather than forgotten; and the open-questions list is empty again. That round trip — *ask → miss →
record the gap → learn (ambiguously) → confirm → answer* — is the entire thesis of this document, and
it cost about twenty commands.

### Where the session landed

```text
> GRAPH_DEGREE id=cat:luna direction=both type=* weighted=1
SUCCESS,id=cat:luna,direction=both,type=*,degree=7,weighted_degree=7.000000
> PAIR_SUMMARY x01676e3a 1 8
SUCCESS,command=PAIR_SUMMARY,count=13,total_payload_bytes=1681,min_payload_bytes=106,max_payload_bytes=199,min_key=2,max_key=70,max_depth=35,self_terminal=0,branch_count=5,branches=59:5;63:4;61:2;5a:1;64:1
```

Note `weighted_degree` is now exactly the degree: with confidence stored in its own field, `weight`
stays at its default and means only "traversal strength". Uncertainty no longer distorts graph
statistics — which is precisely why it stopped living there.

---

## 7. Uncertainty and ambiguity

An agent that learns from conversation is wrong sometimes, half-sure often, and outright confused
regularly. The engine carries all three states on the edge itself — see
[README → Uncertainty and ambiguity](../README.md#uncertainty-and-ambiguity) for the full surface.
What matters for the loop:

**Say how sure you are, in either notation.** `confidence=` takes a number or a word from the ordered
scale `ruled_out < unlikely < possible < probable < certain`; the missing one is derived. A model
that phrases the user's hedge as a word ("she *may* be sterile" → `possible`) and a model that emits a
softmax score both end up in the same store:

```text
> GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=possible props={"src":"1"}
SUCCESS,edge_set,id=MXxjYXQ6bHVuYXxoYXNfY29uZGl0aW9ufGNvbmRpdGlvbjpzdGVyaWxl
> GRAPH_EDGE_GET from=cat:luna to=condition:sterile type=has_condition
# decodes to: {…,"weight":1,"confidence":0.5,"modality":"possible","props":{"src":"1"},…}
```

**Four states, not two.** A learning agent must keep these distinguishable, because each implies a
different next move:

| State | How it looks | What the agent should do |
| --- | --- | --- |
| asserted | no `confidence` declared, or `certain` | answer with it |
| hedged | `possible` / `probable` | answer *and* flag the hedge; consider confirming |
| ruled out | `ruled_out` (0.0) | answer the negative, and stop re-asking |
| unknown | no edge at all (`matches=0`) | say so, record a `hypothesis:` node (§6, turn 5) |

Collapsing "ruled out" into "unknown" — by deleting instead of demoting — is the mistake that makes an
assistant re-ask a question the user already answered.

**Ambiguity is a group, not a guess.** When the sentence offers several readings, write them all:

```text
> GRAPH_AMBIGUITY_SET from=person:marco type=likes group=fav_color options=color:light_blue,color:aquamarine
SUCCESS,ambiguity_set,group=fav_color,options=2,confidence_sum=1.0000
> GRAPH_AMBIGUITY_GET from=person:marco group=fav_color
SUCCESS,group=fav_color,count=2,confidence_sum=1.0000,top=color:aquamarine,top_modality=possible,payload=<base64>
```

`top`/`top_modality` are there for the moment a model *must* pick one: it gets the strongest reading
and the word for how much to trust it, without decoding the payload. Lean one way with
`options=city:lisbon=0.7,city:porto` (the rest splits the leftover) or with relative shares
(`options=a=3,b=1` → 0.75/0.25).

**The words are a filter.** Because the scale is ordered, the same query shape asks "what can I
assert?" before and after the doubt is settled:

```text
> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.modality >= 'probable' RETURN count
SUCCESS,return=count,matches=0,next_cursor=*
> GRAPH_AMBIGUITY_RESOLVE from=person:marco group=fav_color winner=color:aquamarine
SUCCESS,ambiguity_resolved,group=fav_color,winner=color:aquamarine,ruled_out=1,dropped=0
> GRAPH_QUERY MATCH (id='person:marco')-[:likes]->(*) WHERE edge.modality >= 'probable' RETURN paths LIMIT 8
SUCCESS,return=paths,matches=1,next_cursor=*,payload=<base64>
# decodes to: [{"from":"person:marco","type":"likes","to":"color:aquamarine","weight":1}]
```

That is the retrieval-time contract worth building the answer template on: **filter by
`edge.modality >= 'probable'` for what you are willing to state, and query the rest only when the user
asks what you are unsure about.**

---

## 8. Learning to route: prediction tables

The graph stores *what is true*. Prediction tables store *what usually follows* — which intent a
phrasing carries, which action an intent needs, which continuation a token has. They are trained
online, one call per confirmed turn, which is what "learns in real time" means outside the graph.

Below: an intent router under the key `router`, three candidate intents, trained from a 4-feature
context vector. Contexts are base64-encoded JSON — here `[[1,0,1,0]]` stands for a bag of features
like *pet-related / work-related / wants-something / medical*.

**Seed the candidates** (uniform prior — the model knows the options, not the mapping):

```text
> PREDICT_SET key=router value=breed_litter prob=0.34 table=intents
SUCCESS,table=intents,prediction_values=1
> PREDICT_SET key=router value=vet_visit prob=0.33 table=intents
SUCCESS,table=intents,prediction_values=2
> PREDICT_SET key=router value=adopt_pet prob=0.33 table=intents
SUCCESS,table=intents,prediction_values=3

> PREDICT_QUERY key=router ctx=W1sxLCAwLCAxLCAwXV0= table=intents
SUCCESS,count=3,backend=cpu,table=intents,items=62726565645f6c6974746572:0.3356;7665745f7669736974:0.3322;61646f70745f706574:0.3322
# items are <hex value>:<probability> — 62726565645f6c6974746572 is "breed_litter"
```

Nothing is learned yet: three near-identical probabilities, and the context makes no difference.

**Train on a confirmed turn** — target = the intent the user actually meant, `negatives` = the ones it
was not:

```text
> PREDICT_TRAIN key=router target=breed_litter ctx=W1sxLCAwLCAxLCAwXV0= lr=0.4 negatives=vet_visit,adopt_pet table=intents
SUCCESS,table=intents,prediction_values=3,lr=0.4000
   (×3, once per confirmation)

> PREDICT_QUERY key=router ctx=W1sxLCAwLCAxLCAwXV0= table=intents
SUCCESS,count=3,backend=cpu,table=intents,items=62726565645f6c6974746572:0.9306;7665745f7669736974:0.0347;61646f70745f706574:0.0347
```

0.3356 → **0.9306** after three examples. Now teach a *different* context to mean something else:

```text
> PREDICT_TRAIN key=router target=vet_visit ctx=W1swLCAxLCAwLCAxXV0= lr=0.4 negatives=breed_litter,adopt_pet table=intents
   (×3)

> PREDICT_QUERY key=router ctx=W1swLCAxLCAwLCAxXV0= table=intents
SUCCESS,count=3,backend=cpu,table=intents,items=7665745f7669736974:0.9122;62726565645f6c6974746572:0.0542;61646f70745f706574:0.0336

> PREDICT_QUERY key=router ctx=W1sxLCAwLCAxLCAwXV0= table=intents
SUCCESS,count=3,backend=cpu,table=intents,items=62726565645f6c6974746572:0.7062;7665745f7669736974:0.2465;61646f70745f706574:0.0473

> PREDICT_QUERY key=router table=intents
SUCCESS,count=3,backend=cpu,table=intents,items=7665745f7669736974:0.4572;62726565645f6c6974746572:0.3426;61646f70745f706574:0.2003
```

Three things to read off those numbers, all of them operationally important:

1. **The same key gives different answers per context** — 0.91 for `vet_visit` under one vector, 0.71
   for `breed_litter` under the other. That is the context matrix doing its job.
2. **Cross-training interferes**: `breed_litter` under its own context fell from 0.93 to 0.71 after the
   other intent was trained. Interference is the cost of a shared key; if two domains must not bleed
   into each other, give them separate keys or separate `table=` names.
3. **A context-free query returns the blended prior** (`vet_visit` 0.46), not the last thing trained.
   Always pass `ctx=` at inference if you passed one at training.

Supporting commands, verified:

```text
> PREDICT_CTX key=router ctx=W1sxLCAwLCAxLCAwXV0= mode=bias strength=1 table=intents
SUCCESS,table=intents,prediction_values=3
# one-shot bias without training — for "just for this conversation" adjustments

> PREDICT_INHERIT key=router target=pet_intent sources=breed_litter,vet_visit merge=avg table=intents
SUCCESS,table=intents,prediction_values=4,merged_sources=2
# seed a new, coarser candidate from existing ones instead of starting at uniform

> PREDICT_BACKEND table=intents
SUCCESS,table=intents,backend=cpu
> PREDICT_BENCH samples=64 window=8 table=intents
SUCCESS,table=intents,samples=64,window=8,bench=cpu=1.25µs|webgpu-simulated=17.667µs
# timings and field order vary per run; a second run gave webgpu-simulated=29.417µs|cpu=1.125µs
```

Note the bench: `webgpu-simulated` is a CPU fan-out stand-in and was ~14–26× *slower* than `cpu` at
this size across runs. Treat the GPU path as scaffolding, not acceleration.

**When to train.** Only on evidence: the user confirmed, acted, or corrected. Training on your own
guess is how a router converges on its own bias. One call per confirmed turn, `lr` around 0.1–0.4 for
fast adaptation with few examples; lower it as the table matures.

---

## 9. Consolidation, decay and forgetting

**Consolidation (episodic → semantic).** Periodically replay a window of episodes and re-extract with
the fuller context that only exists after the fact — an anaphor resolved three turns later, an entity
that turned out to matter. `PAIR_REDUCE counts episode:<day>` returns the raw utterances inline
(§2.1); feed a page at a time back through the extractor and write with `GRAPH_EDGE_SET_BATCH`. For
long sweeps use `PAIR_REDUCE_ASYNC` and poll `PAIR_REDUCE_STATUS`, so the interactive connection stays
free.

**Fact decay.** Nothing expires by itself. A facts-get-stale policy is a re-assert with a lower weight
and an updated `as_of` — and because omitted fields default (§4.1), the re-assert must carry the
whole record. This is distinct from the bounded *activation decay* in §5.1: adaptive recall changes
how far evidence spreads for one query; it never edits or expires the underlying edge.

**Forgetting, three levels:**

```text
> GRAPH_EDGE_DEL from=person:owner to=hypothesis:catsitter type=unsure_about
SUCCESS,edge_deleted,id=…                     # one relation
> GRAPH_NODE_DEL id=cat cascade=1
SUCCESS,node_deleted,id=cat                    # an entity and its incident edges
> PAIR_PURGE episode: 4096
SUCCESS,purged=2                               # a whole namespace, batched
> PAIR_SCAN episode: 5
SUCCESS,count=0
```

`PAIR_PURGE` deletes the pair entries *and* the backing payloads, which makes "forget everything I told
you today" a single command when episodes are keyed by timestamp (`PAIR_PURGE episode:20260722`).
There is no undo, and no transaction: a purge that is interrupted leaves the prefix partially deleted.

---

## 10. The adapter contract

The thin layer between the model and the socket has a handful of hard rules. All verified.

- **One command per line, one line per response.** Write `<command>\n`, read exactly one line. Do not
  pipeline unless you match responses by order.
- **`LOG_FLUSH` breaks that contract.** Its response is `SUCCESS,count=<n>` followed by `<n>`
  additional `\n`-separated lines, so a line-oriented client desynchronizes — the next command reads a
  log line as its answer:

  ```text
  > LOG_FLUSH 3
  SUCCESS,count=2
  > GRAPH_DEGREE id=cat:luna direction=out type=*
  [1] 2026/07/22 19:25:51.429773 [INFO] Connection closed for 127.0.0.1:51165   ← wrong line
  ```

  Keep `LOG_FLUSH` off the agent's connection (use a separate diagnostic one), or read `count` extra
  lines after it. Every other command in this document answers on exactly one line.
- **Decode `payload=` before reasoning.** It is base64 JSON — a record, or an array of records.
- **Never build a command by string-concatenating user text.** Ids, labels and types are
  whitespace-split tokens; props with spaces must be base64. Slug and encode in the adapter, not in the
  prompt.
- **Page, do not raise `LIMIT`.** Follow `next_cursor` (`*` means exhausted). An unbounded fan-out is
  how a conversation blows its context window.
- **Batch writes** (`GRAPH_EDGE_SET_BATCH`) and keep one long-lived connection: the payload cache is
  per-process and warm caches are most of the latency story.
- **Read back-pressure from the engine**, don't guess it:

  ```text
  > SYSTEM_STATS
  SUCCESS,command=SYSTEM_STATS,timestamp=…,logical_cores=8,gomaxprocs=8,goroutines=57,…,recommended_workers=1:1;32:8;256:8;4096:8,payload_cache_enabled=1,payload_cache_entries=64,payload_cache_max_entries=16384,payload_cache_bytes=6601,payload_cache_max_bytes=67108864,payload_cache_hits=60,payload_cache_misses=0,payload_cache_evictions=13,payload_cache_hit_pct=100.00,payload_cache_advisory_bypass_bytes=1048576
  ```

  `payload_cache_hit_pct` and `recommended_workers` are the two numbers worth reacting to: a falling
  hit rate means the working set outgrew the cache (raise `CHEETAH_PAYLOAD_CACHE_MB`), and the worker
  hints size any background consolidation sweep.
- **Checkpoint before shutdown** if the process is killed rather than closed: `FILE_CHECKPOINT` →
  `SUCCESS,file_checkpoint_flushed=54`.

---

## 11. Failure modes, verified

```text
> GRAPH_NODE_SET id=cat sitter labels=person
SUCCESS,node_set,id=cat
# "sitter" was dropped: args are whitespace-split key=value tokens. Slug ids in the adapter.

> GRAPH_NODE_SET id=person:sara props={"name":"Sara Q"}
ERROR,invalid_props:unexpected end of JSON input
# the space ended the token — base64-encode any props containing spaces

> GRAPH_QUERY MATCH (*)-[:owns]->(id='cat:luna') RETURN edges LIMIT 8
ERROR,graph_query_parse_failed:left_node_must_be_anchored_by_id
# keep the anchor on the left and flip the arrow: (id='cat:luna')<-[:owns]-(*)

> GRAPH_NODE_GET id=cat:pepper
ERROR,node_not_found
# an answer, not a failure: say so, and consider recording a hypothesis: node
```

Plus the two silent ones, which are worse because nothing errors: **payload keys are global** (§3) and
**omitted fields are reset, not preserved** (§4.1).

---

## 12. Prompt scaffolding

Two roles, two prompts. Keeping them separate stops the extractor from "helpfully" answering and the
answerer from inventing writes.

**Teacher (statement → commands):**

```text
You convert user statements into cheetah-db commands. Emit only commands, one per line.

1. Always start with: INSERT:<len> <verbatim utterance>, then PAIR_SET episode:<ts>/<seq> <key
   returned by INSERT>. Use the returned key — never a counter of your own.
2. Node per entity, id "<type>:<slug>" (lowercase, underscores, no spaces). Reuse ids you have
   already used in this conversation; do not mint a variant.
3. labels = kinds; props = compact JSON with no spaces (base64 if any value has a space).
4. Edge per relation: snake_case verb, weight = confidence 0..1, props.src = the INSERT key.
5. Hedges ("may", "I think") → confidence=possible (or probable/unlikely, or a number 0..1).
   Omit confidence entirely for a flat assertion. Provenance goes in props: {"source":"elena"}.
6. "Either A or B" → one GRAPH_AMBIGUITY_SET from=<anchor> group=<slug> options=A,B (add =<share>
   when you lean one way), and GRAPH_AMBIGUITY_RESOLVE ... winner=<id> when it is settled.
7. To change an existing fact, write the COMPLETE record: omitted weight resets to 1.0 and omitted
   props are replaced, not merged (confidence/modality/ambiguity are the exception: they persist).
8. Never write a relation the user did not state. Never write an answer you inferred.
```

**Recaller (question → queries → grounded answer):**

```text
You answer from the database only. Emit queries, read the rows, then answer.

1. Resolve the anchor entity id from the question (possessives resolve through the speaker's edges).
2. Probe cheapest-first: GRAPH_NODE_GET / GRAPH_NEIGHBOR_TYPES / GRAPH_DEGREE. Stop as soon as the
   question is answered.
3. Then the targeted read: GRAPH_NEIGHBORS (one hop; direction=in for reverse) or GRAPH_QUERY
   (WHERE predicates, HOPS 1..n, RETURN edges|nodes|paths|count).
3b. When the question names no relation ("what about X and Y?", "anything I'm missing?"), use
   GRAPH_RECALL seeds=<every entity the turn touched> instead of guessing a query, and add
   min_sources=2 when the question is about what two topics share. State only associations whose
   `via` path you can quote; treat the rest as leads to explore, not as facts.
4. Report edge.modality in words: certain = flat statement, probable/possible = "you mentioned it
   might be", ruled_out = "you later corrected this". Filter with WHERE edge.modality >= 'probable'
   for anything you are going to state as fact.
5. matches=0 / node_not_found = "I don't have that." Never fill the gap from your own knowledge.
   Record the gap: hypothesis:<slug> + <speaker> -[:unsure_about]-> it.
6. End a resolved turn with the write-back: intent -[:blocked_by]-> …, -[:requires]-> …, or the
   new fact itself.
```

---

## 13. Evaluating whether it actually learns

A memory that grows is not the same as a memory that helps. Four measurements, all obtainable from the
database itself:

| Question | Measurement |
| --- | --- |
| Does it remember? | replay N taught facts as questions; count answers grounded in rows |
| Does it over-write? | `created` vs `updated` from `GRAPH_EDGE_SET_BATCH`; a high create rate on repeated topics means the model is minting id variants |
| Does it admit ignorance? | rate of `matches=0` turns answered as "I don't know" vs answered anyway (the failure that matters most) |
| Does it grow sanely? | `PAIR_SUMMARY` node count and `GRAPH_DEGREE` on hub entities over time — a hub whose degree grows without new topics means the extractor is duplicating relations |

For a full harness with ranking metrics (ROC-AUC, average precision, precision@k) over a real dataset,
see [`demo/graph-nell/`](../demo/graph-nell/README.md): it drives a running server over TCP, ingests
edges in batches, and scores link prediction — the same loop as above with a benchmark instead of a
conversation.

---

## 14. Known limits

Design honestly around these; none of them has a workaround inside the server today.

- **No `OR`/`NOT`/parentheses in `WHERE`** — predicates are AND-only. A disjunction is therefore
  stored as an ambiguity *group* and read back with one `edge.ambiguity` equality; anything richer is
  a client-side union.
- **Ambiguity groups are anchored to one node** and are normalized only when written through
  `GRAPH_AMBIGUITY_SET`/`_RESOLVE`; a plain `GRAPH_EDGE_SET` on a member can unbalance the group.
- **No atomicity.** Each command commits on its own; a batch that half-fails leaves half-written state,
  and there is no rollback. Make writes idempotent (they are upserts) and re-run.
- **Recall discovers, it does not consolidate.** `GRAPH_RECALL` never writes back what it found, so an
  association re-derived every turn costs the same every turn; and its lexical matching weighs every
  word equally, with no tolerance for misspellings — a seed that is a typo resolves to nothing.
  Both are open items in `NEXT_STEPS.md`.
- **No vector search.** The trie is prefix-ordered, not metric. Coarse-bucketing a quantized embedding
  into a key prefix does give a usable *candidate* scan — mechanically verified:

  ```text
  > PAIR_SET emb:313102/cat:luna 1
  > PAIR_SET emb:313002/cat:milo 1
  > PAIR_SET emb:023131/person:sara 1
  > PAIR_SCAN emb:31 8
  SUCCESS,count=2,items=656d623a3331333030322f6361743a6d696c6f:1;656d623a3331333130322f6361743a6c756e61:1
  > PAIR_SCAN emb:3131 8
  SUCCESS,count=1,items=656d623a3331333130322f6361743a6c756e61:1
  ```

  — but recall quality of such a bucketing is untested here and it is not a substitute for an ANN
  index. Keep the embeddings and the neighbour search outside, store only the resulting ids.
- **No temporal validity.** `as_of`/`since` props are a convention; the engine never reasons about
  time.
- **Jobs are process-local.** `PAIR_REDUCE_ASYNC` / `PREDICT_INHERIT_ASYNC` /
  `GRAPH_RECALL_ASYNC` results and their retrieval ids vanish on restart.
- **No authentication or TLS.** The protocol is plaintext on `0.0.0.0:4455`; an agent with a socket has
  full write access to every database. Bind it to loopback.
