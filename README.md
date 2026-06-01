# songrec

Music recommendation on the Million Song Dataset (Echo Nest Taste Profile,
~48M listening records) using **item-item collaborative filtering**. The core
song-to-song cosine similarity is implemented from scratch with **Spark RDDs**
only.

The idea: given the songs a listener has played, recommend songs they have not
heard by finding songs that tend to be co-listened with the ones they like, and
show this beats just recommending the most popular songs to everyone.

## Layout

- `src/main/scala/songrec/Pipeline.scala` — the pipeline, including the
  from-scratch RDD item-item cosine similarity (`itemItemSimilarity`).
- `src/main/scala/songrec/App.scala` — end-to-end driver (profiles, model, recommendations).
- `src/main/scala/songrec/Evaluate.scala` — held-out evaluation vs a popularity baseline.
- `src/main/scala/songrec/Common.scala` — parameters and play-count transform.
- `data/sample_triplets.txt` — small synthetic sample for local testing.

## Data

Tab-delimited `userId<TAB>songId<TAB>playCount`, one record per line. Full data:
`train_triplets.txt` from http://millionsongdataset.com/tasteprofile/ .

## Build and run (Cal Poly cluster)

Built with **sbt 1.0.4, Scala 2.11.8, Java 1.8**.

```
sbt package
spark-submit --class songrec.App --master yarn \
  ./target/scala-2.11/songrec_2.11-0.1.jar /user/<you>/input /user/<you>/output
```

`input` is a directory of triplet files; `output` is written with the per-song
similarity model and per-user recommendations.

To evaluate against the popularity baseline (precision/recall/MAP@k):

```
spark-submit --class songrec.Evaluate --master yarn \
  ./target/scala-2.11/songrec_2.11-0.1.jar /user/<you>/input /user/<you>/output
```

Tuning knobs (sparsity filters, per-user cap, co-occurrence pruning, top-K/top-N,
play-count transform) live in `Params` in `Common.scala`.
