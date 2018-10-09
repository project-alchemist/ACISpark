package alchemist.io

import alchemist.AlchemistException


private[alchemist] case class ProtocolException(exitText: String = "")
  extends AlchemistException(s"Protocol Exception: $exitText")
