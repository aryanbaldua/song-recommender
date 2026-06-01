package songrec

case class Params(
  minSongPlays: Int = 3,
  minUserSongs: Int = 5,
  maxSongsPerUser: Int = 200,
  minCoOccur: Int = 3,
  topK: Int = 50,
  topN: Int = 10,
  transform: String = "log",
  seed: Long = 42L
)

object Common {

  def parseTriplet(line: String): Option[(String, String, Int)] = {
    val parts = line.split("\t")
    if (parts.length == 3) {
      try Some((parts(0), parts(1), parts(2).toInt))
      catch { case _: NumberFormatException => None }
    } else None
  }

  def score(count: Int, transform: String): Double = transform match {
    case "raw"      => count.toDouble
    case "binarize" => 1.0
    case "cap"      => math.min(count.toDouble, 10.0)
    case _          => math.log(1.0 + count)
  }
}
