package songrec

import org.apache.spark.rdd.RDD

object Pipeline {

  def buildProfiles(raw: RDD[(String, String, Int)], p: Params)
      : (RDD[(Int, Array[(Int, Double)])], RDD[(Int, String)], RDD[(Int, String)]) = {

    val sc = raw.sparkContext

    val userIds = raw.map(_._1).distinct().zipWithIndex().map { case (h, i) => (h, i.toInt) }
    val songIds = raw.map(_._2).distinct().zipWithIndex().map { case (h, i) => (h, i.toInt) }

    val indexed = raw.map { case (u, s, c) => (u, (s, c)) }
      .join(userIds)
      .map { case (u, ((s, c), ui)) => (s, (ui, c)) }
      .join(songIds)
      .map { case (s, ((ui, c), si)) => (ui, si, c) }

    val keepSongs = sc.broadcast(
      indexed.map(t => (t._2, 1)).reduceByKey(_ + _)
        .filter(_._2 >= p.minSongPlays).map(_._1).collect().toSet
    )
    val bySong = indexed.filter(t => keepSongs.value.contains(t._2))

    val keepUsers = sc.broadcast(
      bySong.map(t => (t._1, 1)).reduceByKey(_ + _)
        .filter(_._2 >= p.minUserSongs).map(_._1).collect().toSet
    )
    val filtered = bySong.filter(t => keepUsers.value.contains(t._1))

    val transform = p.transform
    val maxSongs = p.maxSongsPerUser
    val profiles = filtered
      .map { case (u, s, c) => (u, (s, Common.score(c, transform))) }
      .groupByKey()
      .mapValues { it =>
        val arr = it.toArray
        if (arr.length > maxSongs) arr.sortBy(-_._2).take(maxSongs) else arr
      }

    val userMap = userIds.map { case (h, i) => (i, h) }
    val songMap = songIds.map { case (h, i) => (i, h) }
    (profiles, userMap, songMap)
  }

  def itemItemSimilarity(profiles: RDD[(Int, Array[(Int, Double)])], p: Params)
      : RDD[(Int, Array[(Int, Double)])] = {

    val sc = profiles.sparkContext

    val norms = profiles
      .flatMap { case (_, arr) => arr.map { case (s, w) => (s, w * w) } }
      .reduceByKey(_ + _)
      .mapValues(math.sqrt)
    val normBc = sc.broadcast(norms.collectAsMap())

    val pairs = profiles.flatMap { case (_, arr) =>
      val items = arr.sortBy(_._1)
      val out = scala.collection.mutable.ArrayBuffer[((Int, Int), (Double, Int))]()
      var i = 0
      while (i < items.length) {
        var j = i + 1
        while (j < items.length) {
          out += (((items(i)._1, items(j)._1), (items(i)._2 * items(j)._2, 1)))
          j += 1
        }
        i += 1
      }
      out
    }

    val minCo = p.minCoOccur
    val topK = p.topK

    val aggregated = pairs
      .reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }
      .filter(_._2._2 >= minCo)

    val sims = aggregated.flatMap { case ((a, b), (dot, _)) =>
      val na = normBc.value.getOrElse(a, 0.0)
      val nb = normBc.value.getOrElse(b, 0.0)
      if (na > 0.0 && nb > 0.0) {
        val sim = dot / (na * nb)
        Iterator((a, (b, sim)), (b, (a, sim)))
      } else Iterator.empty
    }

    sims.groupByKey().mapValues { it => it.toArray.sortBy(-_._2).take(topK) }
  }

  def recommend(
      profiles: RDD[(Int, Array[(Int, Double)])],
      model: RDD[(Int, Array[(Int, Double)])],
      topN: Int): RDD[(Int, Array[(Int, Double)])] = {

    val userSong = profiles.flatMap { case (u, arr) => arr.map { case (s, w) => (s, (u, w)) } }

    val scored = userSong.join(model).flatMap { case (_, ((u, w), nbrs)) =>
      nbrs.map { case (n, sim) => ((u, n), w * sim) }
    }.reduceByKey(_ + _)

    val heard = profiles.flatMap { case (u, arr) => arr.map { case (s, _) => ((u, s), 1) } }

    scored.leftOuterJoin(heard)
      .filter(_._2._2.isEmpty)
      .map { case ((u, s), (w, _)) => (u, (s, w)) }
      .groupByKey()
      .mapValues { it => it.toArray.sortBy(-_._2).take(topN) }
  }
}
