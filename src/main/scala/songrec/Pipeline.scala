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
}
