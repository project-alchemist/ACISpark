package alchemist

class WorkerInfo(
    val ID: Short = 0,
    val hostname: String = "",
    val address: String = "",
    val port: Short = 0,
    val groupID: Short = 0
) {

  def toString(includeAllocation: Boolean = true): String = {
    var s = f"Worker-$ID%03d running on $hostname at $address:$port"
    if (includeAllocation)
      if (groupID > 0)
        s += s" - ACTIVE (group $groupID)"
      else
        s += s" - IDLE"

    s
  }
}
