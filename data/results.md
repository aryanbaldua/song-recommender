# Results

Full Million Song Dataset (Echo Nest Taste Profile), run on the Cal Poly YARN
cluster with Spark 2.3.2.

## Setup

- Input: 48,373,586 listening records, 1,019,318 users, 384,546 songs (2.8 GB).
- Evaluation: for every user, half their listening history is hidden and the
  model must recover it from the visible half. Reported over **1,019,139 users**.
- `k = 10` (top-10 recommendations per user).
- Baseline: recommend the globally most-played songs to everyone.

## Headline metrics @10

| Metric | Item-item model | Popularity baseline | Lift |
|---|---|---|---|
| Precision@10 | **0.23276** | 0.04373 | 5.3x |
| Recall@10 | **0.13664** | 0.02800 | 4.9x |
| MAP@10 | **0.18132** | 0.02539 | 7.1x |
| F1@10 | **0.17220** | 0.03414 | 5.0x |

The personalized model beats the popularity baseline by roughly 5-7x on every
metric, across the entire user base. It is learning real co-listening structure,
not guessing.

## What the numbers mean

- Precision@10 = 0.233: about 2.3 of the 10 recommended songs were songs the user
  actually played in the hidden half.
- Recall@10 = 0.137: the 10 recommendations covered about 14% of everything the
  user actually went on to play.
- MAP@10 = 0.181: precision that also rewards ranking the correct songs higher in
  the list.

## Scale (why it needs a cluster)

- Co-occurrence pair generation produced ~387 million song pairs (~5.2 GB shuffle)
  from the 48M input records, the n-choose-2 explosion the design controls with a
  per-user cap and co-occurrence pruning.
- End-to-end full-dataset evaluation: ~1h37m.
