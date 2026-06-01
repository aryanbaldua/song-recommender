package songrec

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = if (args.length > 0) args(0) else "data/sample_triplets.txt"
    val output = if (args.length > 1) args(1) else "output"

    val conf = new SparkConf().setAppName("songrec")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val p = Params()

    val raw = sc.textFile(input).flatMap(Common.parseTriplet)
    val (profiles, userMap, songMap) = Pipeline.buildProfiles(raw, p)
    profiles.cache()

    profiles
      .flatMap { case (u, arr) => arr.map { case (s, w) => s"$u\t$s\t$w" } }
      .saveAsTextFile(output + "/profiles")

    val model = Pipeline.itemItemSimilarity(profiles, p)
    model.cache()

    model
      .flatMap { case (s, nbrs) => nbrs.map { case (n, sim) => s"$s\t$n\t$sim" } }
      .saveAsTextFile(output + "/model")

    val recs = Pipeline.recommend(profiles, model, p.topN)
    recs
      .flatMap { case (u, items) => items.map { case (s, w) => s"$u\t$s\t$w" } }
      .saveAsTextFile(output + "/recommendations")

    sc.stop()
  }
}
