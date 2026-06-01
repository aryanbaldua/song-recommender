package songrec

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object Evaluate {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = if (args.length > 0) args(0) else "data/sample_triplets.txt"
    val output = if (args.length > 1) args(1) else "output"

    val conf = new SparkConf().setAppName("songrec-evaluate")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val p = Params()
    val k = p.topN

    val raw = sc.textFile(input).map(Common.parseTriplet).filter(_.isDefined).map(_.get)
    val (profiles, _, _) = Pipeline.buildProfiles(raw, p)

    val split = Pipeline.splitProfiles(profiles, p.seed)
    split.cache()

    val visible = split.map { case (u, (vis, _)) => (u, vis) }
    val held = split.map { case (u, (_, h)) => (u, h) }
    held.cache()

    val model = Pipeline.itemItemSimilarity(visible, p)
    val recs = Pipeline.recommend(visible, model, k).map { case (u, items) => (u, items.map(_._1)) }
    val (mp, mr, mmap) = Pipeline.metrics(recs, held, k)

    val popular = sc.broadcast(Pipeline.popularSongs(visible, k + p.maxSongsPerUser))
    val baseRecs = visible.map { case (u, vis) =>
      val heardSet = vis.map(_._1).toSet
      (u, popular.value.filterNot(heardSet.contains).take(k))
    }
    val (bp, br, bmap) = Pipeline.metrics(baseRecs, held, k)

    val report = Array(
      s"users_evaluated\t${held.count()}",
      s"k\t$k",
      f"model_precision@$k\t$mp%.5f",
      f"model_recall@$k\t$mr%.5f",
      f"model_map@$k\t$mmap%.5f",
      f"popularity_precision@$k\t$bp%.5f",
      f"popularity_recall@$k\t$br%.5f",
      f"popularity_map@$k\t$bmap%.5f"
    )
    report.foreach(println)
    sc.parallelize(report, 1).saveAsTextFile(output + "/metrics")

    sc.stop()
  }
}
